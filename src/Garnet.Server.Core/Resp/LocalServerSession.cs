// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using Tsavorite;

namespace Garnet.Server;

using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

/// <summary>
/// Local server session
/// </summary>
public class LocalServerSession : IDisposable
{
    private readonly GarnetSessionMetrics sessionMetrics;
    private readonly GarnetLatencyMetricsSession LatencyMetrics;
    private readonly ILogger logger = null;
    private readonly StoreWrapper storeWrapper;
    private readonly StorageSession storageSession;
    private readonly ScratchBufferManager scratchBufferManager;

    /// <summary>
    /// Basic Garnet API
    /// </summary>
    public BasicGarnetApi BasicGarnetApi;

    /// <summary>
    /// Create new local server session
    /// </summary>
    public LocalServerSession(StoreWrapper storeWrapper)
    {
        this.storeWrapper = storeWrapper;

        sessionMetrics = storeWrapper.serverOptions.MetricsSamplingFrequency > 0 ? new GarnetSessionMetrics() : null;
        LatencyMetrics = storeWrapper.serverOptions.LatencyMonitor ? new GarnetLatencyMetricsSession(storeWrapper.monitor) : null;
        logger = storeWrapper.sessionLogger != null ? new SessionLogger(storeWrapper.sessionLogger, $"[local] [local] [{GetHashCode():X8}] ") : null;

        logger?.LogDebug("Starting LocalServerSession");

        // Initialize session-local scratch buffer of size 64 bytes, used for constructing arguments in GarnetApi
        scratchBufferManager = new ScratchBufferManager();

        // Create storage session and API
        storageSession = new StorageSession(storeWrapper, scratchBufferManager, sessionMetrics, LatencyMetrics, logger);

        BasicGarnetApi = new BasicGarnetApi(storageSession, storageSession.basicContext, storageSession.objectStoreBasicContext);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        logger?.LogDebug("Disposing LocalServerSession");

        if (storeWrapper.serverOptions.MetricsSamplingFrequency > 0 || storeWrapper.serverOptions.LatencyMonitor)
            storeWrapper.monitor.AddMetricsHistory(sessionMetrics, LatencyMetrics);

        storageSession.Dispose();
    }
}