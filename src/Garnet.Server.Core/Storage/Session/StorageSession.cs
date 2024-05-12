// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// Storage Session - the internal layer that Garnet uses to perform storage operations
/// </summary>
internal sealed partial class StorageSession : IDisposable
{
    private int bitmapBufferSize = 1 << 15; //TODO: ??

    private SectorAlignedMemory _sectorAlignedMemoryBitmap;
    private readonly long _headAddress;

    /// <summary>
    /// Session for main store
    /// </summary>
    public readonly ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> Session;

    public BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> BasicContext { get; }
    public LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> LockableContext { get; }

    private SectorAlignedMemory _sectorAlignedMemoryHll;
    private readonly int _hllBufferSize = HyperLogLog.DefaultHLL.DenseBytes;
    
    private const int SectorAlignedMemoryPoolAlignment = 32;

    /// <summary>
    /// Session for object store
    /// </summary>
    public ClientSession<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> ObjectStoreSession { get; }

    public BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> ObjectStoreBasicContext { get; }
    public LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> ObjectStoreLockableContext { get; }

    public ScratchBufferManager ScratchBufferManager { get; }
    public FunctionsState FunctionsState { get; }

    public TransactionManager txnManager;
    private readonly ILogger logger;

    public int SessionID => Session.ID;
    public int ObjectStoreSessionID => ObjectStoreSession.ID;

    public readonly int ObjectScanCountLimit;

    public StorageSession(StoreWrapper storeWrapper,
        ScratchBufferManager scratchBufferManager,
        GarnetSessionMetrics sessionMetrics,
        GarnetLatencyMetricsSession LatencyMetrics, ILogger logger = null)
    {
        this.sessionMetrics = sessionMetrics;
        this.LatencyMetrics = LatencyMetrics;
        this.ScratchBufferManager = scratchBufferManager;
        this.logger = logger;

        FunctionsState = storeWrapper.CreateFunctionsState();

        var functions = new MainStoreFunctions(FunctionsState);
        Session = storeWrapper.store.NewSession<SpanByte, SpanByteAndMemory, long, MainStoreFunctions>(functions);

        var objstorefunctions = new ObjectStoreFunctions(FunctionsState);
        ObjectStoreSession = storeWrapper.objectStore?.NewSession<SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>(objstorefunctions);

        BasicContext = Session.BasicContext;
        LockableContext = Session.LockableContext;
        if (ObjectStoreSession != null)
        {
            ObjectStoreBasicContext = ObjectStoreSession.BasicContext;
            ObjectStoreLockableContext = ObjectStoreSession.LockableContext;
        }

        _headAddress = storeWrapper.store.Log.HeadAddress;
        ObjectScanCountLimit = storeWrapper.serverOptions.ObjectScanCountLimit;
    }

    public void Dispose()
    {
        _sectorAlignedMemoryBitmap?.Dispose();
        Session.Dispose();
        ObjectStoreSession?.Dispose();
        _sectorAlignedMemoryHll?.Dispose();
    }
}