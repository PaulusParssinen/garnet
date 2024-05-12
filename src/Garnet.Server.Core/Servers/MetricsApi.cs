// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;

namespace Garnet.Server;

/// <summary>
/// Metrics API
/// </summary>
public class MetricsApi
{
    private readonly GarnetProvider provider;

    /// <summary>
    /// Construct new Metrics API instance
    /// </summary>
    public MetricsApi(GarnetProvider provider)
    {
        this.provider = provider;
    }

    /// <summary>
    /// Get info metrics for specified info type
    /// </summary>
    public MetricsItem[] GetInfoMetrics(InfoMetricsType infoMetricsType)
    {
        GarnetInfoMetrics info = new();
        return info.GetMetric(infoMetricsType, provider.StoreWrapper);
    }

    /// <summary>
    /// Get info metrics for specified info types
    /// </summary>
    /// <param name="infoMetricsTypes">Info types to get, null to get all</param>
    public IEnumerable<(InfoMetricsType, MetricsItem[])> GetInfoMetrics(InfoMetricsType[] infoMetricsTypes = null)
    {
        GarnetInfoMetrics info = new();
        infoMetricsTypes ??= GarnetInfoMetrics.defaultInfo;
        return info.GetInfoMetrics(infoMetricsTypes, provider.StoreWrapper);
    }

    /// <summary>
    /// Get header for given info metrics type
    /// </summary>
    public static string GetHeader(InfoMetricsType infoMetricsType)
        => GarnetInfoMetrics.GetSectionHeader(infoMetricsType);

    /// <summary>
    /// Reset info metrics
    /// </summary>
    public void ResetInfoMetrics(InfoMetricsType infoMetricsType)
    {
        if (provider.StoreWrapper.monitor != null)
            provider.StoreWrapper.monitor.resetEventFlags[infoMetricsType] = true;
    }

    /// <summary>
    /// Reset info metrics
    /// </summary>
    /// <param name="infoMetricsTypes">Info types to reset, null to reset all</param>
    public void ResetInfoMetrics(InfoMetricsType[] infoMetricsTypes = null)
    {
        infoMetricsTypes ??= GarnetInfoMetrics.defaultInfo;
        for (int i = 0; i < infoMetricsTypes.Length; i++)
            ResetInfoMetrics(infoMetricsTypes[i]);
    }

    /// <summary>
    /// Get latency metrics (histogram) for specified latency type
    /// </summary>
    public MetricsItem[] GetLatencyMetrics(LatencyMetricsType latencyMetricsType)
    {
        if (provider.StoreWrapper.monitor?.GlobalMetrics.GlobalLatencyMetrics == null) return Array.Empty<MetricsItem>();
        return provider.StoreWrapper.monitor.GlobalMetrics.GlobalLatencyMetrics.GetLatencyMetrics(latencyMetricsType);
    }

    /// <summary>
    /// Get latency metrics (histograms) for specified latency types
    /// </summary>
    /// <param name="latencyMetricsTypes">Latency types to get, null to get all</param>
    public IEnumerable<(LatencyMetricsType, MetricsItem[])> GetLatencyMetrics(LatencyMetricsType[] latencyMetricsTypes = null)
    {
        if (provider.StoreWrapper.monitor?.GlobalMetrics.GlobalLatencyMetrics == null) return Array.Empty<(LatencyMetricsType, MetricsItem[])>();
        latencyMetricsTypes ??= GarnetLatencyMetrics.defaultLatencyTypes;
        return provider.StoreWrapper.monitor?.GlobalMetrics.GlobalLatencyMetrics.GetLatencyMetrics(latencyMetricsTypes);
    }

    /// <summary>
    /// Reset latency histogram for eventType
    /// </summary>
    /// <param name="latencyMetricsType">Latency types to reset, null to reset all</param>
    public void ResetLatencyMetrics(LatencyMetricsType latencyMetricsType)
    {
        if (provider.StoreWrapper.monitor != null)
            provider.StoreWrapper.monitor.resetLatencyMetrics[latencyMetricsType] = true;
    }

    /// <summary>
    /// Reset latency histogram for eventTypes
    /// </summary>
    public void ResetLatencyMetrics(LatencyMetricsType[] latencyMetricsTypes = null)
    {
        latencyMetricsTypes ??= GarnetLatencyMetrics.defaultLatencyTypes;
        for (int i = 0; i < latencyMetricsTypes.Length; i++)
            ResetLatencyMetrics(latencyMetricsTypes[i]);
    }
}