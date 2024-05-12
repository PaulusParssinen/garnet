// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.Common;
using Microsoft.Extensions.Logging;
using Tsavorite;

namespace Garnet.Server;

using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;
using LockableGarnetApi = GarnetApi<LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

/// <summary>
/// Transaction manager
/// </summary>
public sealed unsafe partial class TransactionManager
{
    /// <summary>
    /// Session for main store
    /// </summary>
    private readonly ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> _session;

    /// <summary>
    /// Lockable context for main store
    /// </summary>
    private readonly LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> _lockableContext;

    /// <summary>
    /// Session for object store
    /// </summary>
    private readonly ClientSession<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> _objectStoreSession;

    /// <summary>
    /// Lockable context for object store
    /// </summary>
    private readonly LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> _objectStoreLockableContext;

    // Not readonly to avoid defensive copy
    private GarnetWatchApi<BasicGarnetApi> _garnetTxPrepareApi;

    // Cluster session
    private IClusterSession _clusterSession;

    // Not readonly to avoid defensive copy
    private LockableGarnetApi _garnetTxMainApi;

    // Not readonly to avoid defensive copy
    private BasicGarnetApi _garnetTxFinalizeApi;

    private readonly RespServerSession _respSession;
    private readonly FunctionsState _functionsState;
    private readonly TsavoriteLog _appendOnlyFile;
    
    internal readonly ScratchBufferManager ScratchBufferManager;
    internal readonly WatchedKeysContainer WatchContainer;
    
    internal int TxnStartHead;
    internal int OperationCntTxn;

    /// <summary>
    /// State
    /// </summary>
    public TxnState State;
    
    private const int InitialSliceBufferSize = 1 << 10;
    private const int InitialKeyBufferSize = 1 << 10;
    private StoreType _transactionStoreType;
    
    private readonly ILogger _logger;

    internal LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> LockableContext
        => _lockableContext;
    internal LockableUnsafeContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> LockableUnsafeContext
        => _session.LockableUnsafeContext;
    internal LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> ObjectStoreLockableContext
        => _objectStoreLockableContext;

    /// <summary>
    /// Array to keep pointer keys in keyBuffer
    /// </summary>
    private TxnKeyEntries keyEntries;

    internal TransactionManager(
        RespServerSession respSession,
        StorageSession storageSession,
        ScratchBufferManager scratchBufferManager,
        bool clusterEnabled,
        ILogger logger = null)
    {
        _session = storageSession.Session;
        _lockableContext = _session.LockableContext;

        _objectStoreSession = storageSession.ObjectStoreSession;
        if (_objectStoreSession != null)
            _objectStoreLockableContext = _objectStoreSession.LockableContext;

        _functionsState = storageSession.FunctionsState;
        _appendOnlyFile = _functionsState.AppendOnlyFile;
        _logger = logger;

        _respSession = respSession;
        _clusterSession = respSession.clusterSession;

        WatchContainer = new WatchedKeysContainer(InitialSliceBufferSize, _functionsState.WatchVersionMap);
        keyEntries = new TxnKeyEntries(InitialSliceBufferSize, _lockableContext, _objectStoreLockableContext);
        ScratchBufferManager = scratchBufferManager;

        _garnetTxMainApi = respSession.lockableGarnetApi;
        _garnetTxPrepareApi = new GarnetWatchApi<BasicGarnetApi>(respSession.basicGarnetApi);
        _garnetTxFinalizeApi = respSession.basicGarnetApi;

        this.clusterEnabled = clusterEnabled;
        if (clusterEnabled)
            keys = new ArgSlice[InitialKeyBufferSize];

        Reset(false);
    }

    internal void Reset(bool isRunning)
    {
        if (isRunning)
        {
            keyEntries.UnlockAllKeys();

            // Release context
            if (_transactionStoreType == StoreType.Main || _transactionStoreType == StoreType.All)
                _lockableContext.EndLockable();
            if (_transactionStoreType == StoreType.Object || _transactionStoreType == StoreType.All)
            {
                if (_objectStoreSession == null)
                    throw new Exception("Trying to perform object store transaction with object store disabled");

                _objectStoreLockableContext.EndLockable();
            }
        }
        TxnStartHead = 0;
        OperationCntTxn = 0;
        State = TxnState.None;
        _transactionStoreType = 0;
        _functionsState.StoredProcMode = false;

        // Reset cluster variables used for slot verification
        saveKeyRecvBufferPtr = null;
        keyCount = 0;
    }

    internal bool RunTransactionProc(byte id, ArgSlice input, CustomTransactionProcedure proc, ref MemoryResult<byte> output)
    {
        bool running = false;
        ScratchBufferManager.Reset();
        try
        {
            _functionsState.StoredProcMode = true;
            // Prepare phase
            if (!proc.Prepare(_garnetTxPrepareApi, input))
            {
                Reset(running);
                return false;
            }

            // Start the TransactionManager
            if (!Run(failFastOnLock: proc.FailFastOnKeyLockFailure, lockTimeout: proc.KeyLockTimeout))
            {
                Reset(running);
                return false;
            }

            running = true;

            // Run main procedure on locked data
            proc.Main(_garnetTxMainApi, input, ref output);

            // Log the transaction to AOF
            Log(id, input);

            // Commit
            Commit();
        }
        catch
        {
            Reset(running);
            return false;
        }
        finally
        {
            try
            {
                // Run finalize procedure at the end
                proc.Finalize(_garnetTxFinalizeApi, input, ref output);
            }
            catch { }

            // Reset scratch buffer for next txn invocation
            ScratchBufferManager.Reset();
        }


        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool IsSkippingOperations()
    {
        return State == TxnState.Started || State == TxnState.Aborted;
    }

    internal void Abort()
    {
        State = TxnState.Aborted;
    }

    internal void Log(byte id, ArgSlice input)
    {
        Debug.Assert(_functionsState.StoredProcMode);
        SpanByte sb = new SpanByte(input.Length, (nint)input.ptr);
        _appendOnlyFile?.Enqueue(new AofHeader { OpType = AofEntryType.StoredProcedure, Type = id, Version = _session.Version, SessionId = _session.ID }, ref sb, out _);
    }

    internal void Commit(bool internal_txn = false)
    {
        if (_appendOnlyFile != null && !_functionsState.StoredProcMode)
        {
            _appendOnlyFile.Enqueue(new AofHeader { OpType = AofEntryType.TxnCommit, Version = _session.Version, SessionId = _session.ID }, out _);
        }
        if (!internal_txn)
            WatchContainer.Reset();
        Reset(true);
    }

    internal void Watch(ArgSlice key, StoreType type)
    {
        UpdateTransactionStoreType(type);
        WatchContainer.AddWatch(key, type);

        if (type == StoreType.Main || type == StoreType.All)
            _session.ResetModified(key.SpanByte);
        if (type == StoreType.Object || type == StoreType.All)
            _objectStoreSession?.ResetModified(key.ToArray());
    }

    private void UpdateTransactionStoreType(StoreType type)
    {
        if (_transactionStoreType != StoreType.All)
        {
            if (_transactionStoreType == 0)
                _transactionStoreType = type;
            else
            {
                if (_transactionStoreType != type)
                    _transactionStoreType = StoreType.All;
            }
        }
    }

    internal string GetLockset() => keyEntries.GetLockset();

    internal void GetKeysForValidation(byte* recvBufferPtr, out ArgSlice[] keys, out int keyCount, out bool readOnly)
    {
        UpdateRecvBufferPtr(recvBufferPtr);
        WatchContainer.SaveKeysToKeyList(this);
        keys = this.keys;
        keyCount = this.keyCount;
        readOnly = keyEntries.IsReadOnly;
    }

    internal bool Run(bool internalTxn = false, bool failFastOnLock = false, TimeSpan lockTimeout = default)
    {
        // Save watch keys to lock list
        if (!internalTxn)
            WatchContainer.SaveKeysToLock(this);

        // Acquire lock sessions
        if (_transactionStoreType == StoreType.All || _transactionStoreType == StoreType.Main)
        {
            _lockableContext.BeginLockable();
        }
        if (_transactionStoreType == StoreType.All || _transactionStoreType == StoreType.Object)
        {
            if (_objectStoreSession == null)
                throw new Exception("Trying to perform object store transaction with object store disabled");

            _objectStoreLockableContext.BeginLockable();
        }

        bool lockSuccess;
        if (failFastOnLock)
        {
            lockSuccess = keyEntries.TryLockAllKeys(lockTimeout);
        }
        else
        {
            keyEntries.LockAllKeys();
            lockSuccess = true;
        }

        if (!lockSuccess ||
            (!internalTxn && !WatchContainer.ValidateWatchVersion()))
        {
            if (!lockSuccess)
            {
                _logger?.LogError("Transaction failed to acquire all the locks on keys to proceed.");
            }
            Reset(true);
            if (!internalTxn)
                WatchContainer.Reset();
            return false;
        }

        if (_appendOnlyFile != null && !_functionsState.StoredProcMode)
        {
            _appendOnlyFile.Enqueue(new AofHeader { OpType = AofEntryType.TxnStart, Version = _session.Version, SessionId = _session.ID }, out _);
        }

        State = TxnState.Running;
        return true;
    }
}