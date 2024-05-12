// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.Common;
using Microsoft.Extensions.Logging;

namespace Garnet.Server;

internal enum EventType : byte
{
    COMMAND,
    STATS
}

internal sealed class GarnetServerMonitor
{
    public readonly Dictionary<InfoMetricsType, bool>
        resetEventFlags = GarnetInfoMetrics.defaultInfo.ToDictionary(x => x, y => false);

    public readonly Dictionary<LatencyMetricsType, bool>
        resetLatencyMetrics = GarnetLatencyMetrics.defaultLatencyTypes.ToDictionary(x => x, y => false);
    private readonly StoreWrapper storeWrapper;
    private readonly GarnetServerOptions opts;
    private readonly IGarnetServer server;
    private readonly int monitorTaskDelay;
    public long monitor_iterations;
    private GarnetServerMetrics globalMetrics;
    private readonly GarnetSessionMetrics accSessionMetrics;
    private int instant_metrics_period;
    private ulong instant_input_net_bytes;
    private ulong instant_output_net_bytes;
    private ulong instant_commands_processed;
    private long startTimestamp;
    private readonly CancellationTokenSource cts = new();
    private readonly ManualResetEvent done = new(false);
    private readonly ILogger logger;

    public GarnetServerMetrics GlobalMetrics => globalMetrics;

    private SingleWriterMultiReaderLock rwLock = new();

    public GarnetServerMonitor(StoreWrapper storeWrapper, GarnetServerOptions opts, IGarnetServer server, ILogger logger = null)
    {
        this.storeWrapper = storeWrapper;
        this.opts = opts;
        this.server = server;
        this.logger = logger;
        monitorTaskDelay = opts.MetricsSamplingFrequency * 1000;
        monitor_iterations = 0;

        instant_metrics_period = monitorTaskDelay > 0 ? Math.Max(1000 / monitorTaskDelay, 1) : 0;
        instant_input_net_bytes = 0;
        instant_output_net_bytes = 0;
        instant_commands_processed = 0;
        startTimestamp = 0;
        globalMetrics = new(true, opts.LatencyMonitor, this);

        accSessionMetrics = new GarnetSessionMetrics();
    }

    public void Dispose()
    {
        cts.Cancel();
        done.WaitOne();
        cts.Dispose();
        done.Dispose();
    }

    public void Start()
    {
        Task.Run(() => MainMonitorTask(cts.Token));
    }

    public void AddMetricsHistory(GarnetSessionMetrics currSessionMetrics, GarnetLatencyMetricsSession currLatencyMetrics)
    {
        rwLock.WriteLock();
        try
        {
            if (currSessionMetrics != null) globalMetrics.HistorySessionMetrics.Add(currSessionMetrics);
            if (currLatencyMetrics != null) globalMetrics.GlobalLatencyMetrics.Merge(currLatencyMetrics);
        }
        finally { rwLock.WriteUnlock(); }
    }

    public string GetAllLocksets()
    {
        string result = "";
        IEnumerable<Networking.IMessageConsumer> sessions = ((GarnetServerBase)server).ActiveConsumers();
        foreach (Networking.IMessageConsumer s in sessions)
        {
            var session = (RespServerSession)s;
            string lockset = session.txnManager.GetLockset();
            if (lockset != "")
                result += session.StoreSessionID + ": " + lockset + "\n";
        }
        return result;
    }

    private void UpdateInstantaneousMetrics()
    {
        if (monitor_iterations % instant_metrics_period == 0)
        {
            long currTimestamp = Stopwatch.GetTimestamp();
            double elapsedSec = TimeSpan.FromTicks(currTimestamp - startTimestamp).TotalSeconds;
            globalMetrics.Instantaneous_net_input_tpt = (globalMetrics.GlobalSessionMetrics.get_total_net_input_bytes() - instant_input_net_bytes) / (elapsedSec * GarnetServerMetrics.ByteUnit);
            globalMetrics.Instantaneous_net_output_tpt = (globalMetrics.GlobalSessionMetrics.get_total_net_output_bytes() - instant_output_net_bytes) / (elapsedSec * GarnetServerMetrics.ByteUnit);
            globalMetrics.Instantaneous_cmd_per_sec = (globalMetrics.GlobalSessionMetrics.get_total_commands_processed() - instant_commands_processed) / elapsedSec;

            globalMetrics.Instantaneous_net_input_tpt = Math.Round(globalMetrics.Instantaneous_net_input_tpt, 2);
            globalMetrics.Instantaneous_net_output_tpt = Math.Round(globalMetrics.Instantaneous_net_output_tpt, 2);
            globalMetrics.Instantaneous_cmd_per_sec = Math.Round(globalMetrics.Instantaneous_cmd_per_sec);

            startTimestamp = currTimestamp;
            instant_input_net_bytes = globalMetrics.GlobalSessionMetrics.get_total_net_input_bytes();
            instant_output_net_bytes = globalMetrics.GlobalSessionMetrics.get_total_net_output_bytes();
            instant_commands_processed = globalMetrics.GlobalSessionMetrics.get_total_commands_processed();
        }
    }

    private void UpdateAllMetricsHistory()
    {
        //Reset session metrics accumulator
        accSessionMetrics.Reset();
        //Add session metrics history in accumulator
        accSessionMetrics.Add(globalMetrics.HistorySessionMetrics);
    }

    private void UpdateAllMetrics(IGarnetServer server)
    {
        //Accumulate metrics from all active sessions
        IEnumerable<Networking.IMessageConsumer> sessions = ((GarnetServerBase)server).ActiveConsumers();
        foreach (Networking.IMessageConsumer s in sessions)
        {
            var session = (RespServerSession)s;

            //Accumulate session metrics
            accSessionMetrics.Add(session.GetSessionMetrics);

            // Accumulate latency metrics if latency monitor is enabled
            if (opts.LatencyMonitor)
            {
                rwLock.WriteLock();
                try
                {
                    // Add accumulated latency metrics for this iteration
                    globalMetrics.GlobalLatencyMetrics.Merge(session.GetLatencyMetrics());
                }
                finally
                {
                    rwLock.WriteUnlock();
                }
            }
        }

        // Reset global session metrics
        globalMetrics.GlobalSessionMetrics.Reset();
        // Add accumulated session metrics for this iteration
        globalMetrics.GlobalSessionMetrics.Add(accSessionMetrics);

    }

    private void ResetStats()
    {
        if (resetEventFlags[InfoMetricsType.STATS])
        {
            logger?.LogInformation("Resetting latency metrics for commands");
            globalMetrics.Instantaneous_net_input_tpt = 0;
            globalMetrics.Instantaneous_net_output_tpt = 0;
            globalMetrics.Instantaneous_cmd_per_sec = 0;

            globalMetrics.TotalConnectionsReceived = 0;
            globalMetrics.TotalConnectionsDisposed = 0;
            globalMetrics.GlobalSessionMetrics.Reset();
            globalMetrics.HistorySessionMetrics.Reset();

            var garnetServer = (GarnetServerBase)server;
            IEnumerable<Networking.IMessageConsumer> sessions = garnetServer.ActiveConsumers();
            foreach (Networking.IMessageConsumer s in sessions)
            {
                var session = (RespServerSession)s;
                session.GetSessionMetrics.Reset();
            }

            garnetServer.reset_conn_recv();
            garnetServer.reset_conn_disp();

            storeWrapper.clusterProvider.ResetGossipStats();

            storeWrapper.store.ResetRevivificationStats();
            storeWrapper.objectStore.ResetRevivificationStats();

            resetEventFlags[InfoMetricsType.STATS] = false;
        }
    }

    private void ResetLatencyMetrics()
    {
        if (opts.LatencyMonitor)
        {
            foreach (LatencyMetricsType eventType in resetLatencyMetrics.Keys)
            {
                if (resetLatencyMetrics[eventType])
                {
                    logger?.LogInformation($"Resetting server-side stats {eventType}");

                    IEnumerable<Networking.IMessageConsumer> sessions = ((GarnetServerBase)server).ActiveConsumers();
                    foreach (Networking.IMessageConsumer entry in sessions)
                        ((RespServerSession)entry).ResetLatencyMetrics(eventType);

                    rwLock.WriteLock();
                    try
                    {
                        globalMetrics.GlobalLatencyMetrics.Reset(eventType);
                    }
                    finally
                    {
                        rwLock.WriteUnlock();
                    }

                    resetLatencyMetrics[eventType] = false;
                }
            }
        }
    }

    private void ResetLatencySessionMetrics()
    {
        if (opts.LatencyMonitor)
        {
            IEnumerable<Networking.IMessageConsumer> sessions = ((GarnetServerBase)server).ActiveConsumers();
            foreach (Networking.IMessageConsumer entry in sessions)
                ((RespServerSession)entry).ResetAllLatencyMetrics();
        }
    }

    private async void MainMonitorTask(CancellationToken token)
    {
        startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            while (true)
            {
                await Task.Delay(monitorTaskDelay, token);

                // Reset the session level latency metrics for the prior version, as we are
                // about to make that the current version.
                ResetLatencySessionMetrics();

                monitor_iterations++;

                var garnetServer = (GarnetServerBase)server;
                globalMetrics.TotalConnectionsReceived = garnetServer.get_conn_recv();
                globalMetrics.TotalConnectionsDisposed = garnetServer.get_conn_disp();

                UpdateInstantaneousMetrics();
                UpdateAllMetricsHistory();
                UpdateAllMetrics(server);

                //Reset & Cleanup
                ResetStats();
                ResetLatencyMetrics();
            }
        }
        catch (Exception ex)
        {
            logger?.LogCritical(ex, "MainMonitorTask exception");
        }
        finally
        {
            done.Set();
        }
    }
}