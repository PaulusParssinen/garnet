﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using Garnet.Common;

namespace Garnet.Server;

/// <summary>
/// Latency metrics emitted from RespServerSession
/// </summary>
internal sealed class GarnetLatencyMetricsSession
{
    private readonly GarnetServerMonitor monitor;
    private static readonly LatencyMetricsType[] defaultLatencyTypes = Enum.GetValues<LatencyMetricsType>();

    private int Version => (int)(monitor.monitor_iterations % 2);
    public int PriorVersion => 1 - Version;
    public LatencyMetricsEntrySession[] metrics;

    public GarnetLatencyMetricsSession(GarnetServerMonitor monitor)
    {
        this.monitor = monitor;
        Init();
    }

    private void Init()
    {
        metrics = new LatencyMetricsEntrySession[defaultLatencyTypes.Length];
        foreach (LatencyMetricsType cmd in defaultLatencyTypes)
            metrics[(int)cmd] = new LatencyMetricsEntrySession();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start(LatencyMetricsType cmd)
    {
        int idx = (int)cmd;
        metrics[idx].Start();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void StopAndSwitch(LatencyMetricsType oldCmd, LatencyMetricsType newCmd)
    {
        int old_idx = (int)oldCmd;
        int new_idx = (int)newCmd;
        metrics[new_idx].startTimestamp = metrics[old_idx].startTimestamp;
        metrics[old_idx].startTimestamp = 0;
        metrics[new_idx].RecordValue(Version);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Stop(LatencyMetricsType cmd)
    {
        int idx = (int)cmd;
        metrics[idx].RecordValue(Version);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordValue(LatencyMetricsType cmd, long value)
    {
        int idx = (int)cmd;
        metrics[idx].RecordValue(Version, value);
    }

    public void ResetAll()
    {
        foreach (LatencyMetricsType cmd in defaultLatencyTypes)
            Reset(cmd);
    }

    public void Reset(LatencyMetricsType cmd)
    {
        int idx = (int)cmd;
        metrics[idx].latency[PriorVersion].Reset();
    }
}