// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Server;

internal struct GarnetServerMetrics
{
    /// <summary>
    /// Server metrics
    /// </summary>
    public long TotalConnectionsReceived;
    public long TotalConnectionsDisposed;

    /// <summary>
    /// Instantaneous metrics
    /// </summary>
    public static readonly int ByteUnit = 1 << 10;
    public double Instantaneous_cmd_per_sec;
    public double Instantaneous_net_input_tpt;
    public double Instantaneous_net_output_tpt;

    /// <summary>
    /// Global session metrics
    /// </summary>
    public GarnetSessionMetrics GlobalSessionMetrics;

    /// <summary>
    /// History of session metrics.
    /// </summary>
    public GarnetSessionMetrics HistorySessionMetrics;

    /// <summary>
    /// Global latency metrics per command.
    /// </summary>
    public readonly GarnetLatencyMetrics GlobalLatencyMetrics;

    public GarnetServerMetrics(bool trackStats, bool trackLatency, GarnetServerMonitor monitor)
    {
        TotalConnectionsReceived = 0;
        TotalConnectionsDisposed = 0;

        Instantaneous_cmd_per_sec = 0;
        Instantaneous_net_input_tpt = 0;
        Instantaneous_net_output_tpt = 0;

        GlobalSessionMetrics = trackStats ? new GarnetSessionMetrics() : null;
        HistorySessionMetrics = trackStats ? new GarnetSessionMetrics() : null;

        GlobalLatencyMetrics = trackLatency ? new() : null;
    }
}