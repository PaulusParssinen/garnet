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
    private int bitmapBufferSize = 1 << 15;
    private SectorAlignedMemory sectorAlignedMemoryBitmap;
    private readonly long HeadAddress;

    /// <summary>
    /// Session for main store
    /// </summary>
    public readonly ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> session;

    public BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> basicContext;
    public LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> lockableContext;
    private SectorAlignedMemory sectorAlignedMemoryHll;
    private readonly int hllBufferSize = HyperLogLog.DefaultHLL.DenseBytes;
    private readonly int sectorAlignedMemoryPoolAlignment = 32;

    /// <summary>
    /// Session for object store
    /// </summary>
    public readonly ClientSession<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreSession;

    public BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreBasicContext;
    public LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreLockableContext;

    public readonly ScratchBufferManager scratchBufferManager;
    public readonly FunctionsState functionsState;

    public TransactionManager txnManager;
    private readonly ILogger logger;

    public int SessionID => session.ID;
    public int ObjectStoreSessionID => objectStoreSession.ID;

    public readonly int ObjectScanCountLimit;

    public StorageSession(StoreWrapper storeWrapper,
        ScratchBufferManager scratchBufferManager,
        GarnetSessionMetrics sessionMetrics,
        GarnetLatencyMetricsSession LatencyMetrics, ILogger logger = null)
    {
        this.sessionMetrics = sessionMetrics;
        this.LatencyMetrics = LatencyMetrics;
        this.scratchBufferManager = scratchBufferManager;
        this.logger = logger;

        functionsState = storeWrapper.CreateFunctionsState();

        var functions = new MainStoreFunctions(functionsState);
        session = storeWrapper.store.NewSession<SpanByte, SpanByteAndMemory, long, MainStoreFunctions>(functions);

        var objstorefunctions = new ObjectStoreFunctions(functionsState);
        objectStoreSession = storeWrapper.objectStore?.NewSession<SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>(objstorefunctions);

        basicContext = session.BasicContext;
        lockableContext = session.LockableContext;
        if (objectStoreSession != null)
        {
            objectStoreBasicContext = objectStoreSession.BasicContext;
            objectStoreLockableContext = objectStoreSession.LockableContext;
        }

        HeadAddress = storeWrapper.store.Log.HeadAddress;
        ObjectScanCountLimit = storeWrapper.serverOptions.ObjectScanCountLimit;
    }

    public void Dispose()
    {
        sectorAlignedMemoryBitmap?.Dispose();
        session.Dispose();
        objectStoreSession?.Dispose();
        sectorAlignedMemoryHll?.Dispose();
    }
}