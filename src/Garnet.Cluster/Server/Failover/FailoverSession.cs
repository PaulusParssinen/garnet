﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;
using Microsoft.Extensions.Logging;

namespace Garnet.Cluster;

internal sealed partial class FailoverSession : IDisposable
{
    private readonly ClusterProvider clusterProvider;
    private readonly TimeSpan clusterTimeout;
    private readonly TimeSpan failoverTimeout;
    private readonly CancellationTokenSource cts;
    private readonly FailoverOption option;
    private readonly ILogger logger;
    private readonly GarnetClient[] clients = null;
    private readonly DateTime failoverDeadline;

    public FailoverStatus status { get; private set; }

    public bool FailoverTimeout => failoverDeadline < DateTime.UtcNow;

    private readonly ClusterConfig currentConfig;

    /// <summary>
    /// FailoverSession constructor
    /// </summary>
    /// <param name="clusterProvider">ClusterProvider object</param>
    /// <param name="option">Failover options for replica failover session.</param>
    /// <param name="clusterTimeout">Timeout for individual communication between replica.</param>
    /// <param name="failoverTimeout">End to end timeout for failover</param>
    /// <param name="isReplicaSession">Flag indicating if this session is controlled by a replica</param>
    public FailoverSession(
        ClusterProvider clusterProvider,
        FailoverOption option,
        TimeSpan clusterTimeout,
        TimeSpan failoverTimeout,
        bool isReplicaSession = true,
        string hostAddress = "",
        int hostPort = -1,
        ILogger logger = null)
    {
        this.clusterProvider = clusterProvider;
        this.clusterTimeout = clusterTimeout;
        this.option = option;
        this.logger = logger;
        currentConfig = clusterProvider.clusterManager.CurrentConfig;
        cts = new();

        // Initialize connections only when failover is initiated by the primary
        if (!isReplicaSession)
        {
            List<(string, int)> endpoints = hostPort == -1
                ? currentConfig.GetLocalNodePrimaryEndpoints(includeMyPrimaryFirst: true)
                : hostPort == 0 ? currentConfig.GetLocalNodeReplicaEndpoints() : null;
            clients = endpoints != null ? new GarnetClient[endpoints.Count] : new GarnetClient[1];

            if (clients.Length > 1)
            {
                for (int i = 0; i < endpoints.Count; i++)
                {
                    clients[i] = new GarnetClient(endpoints[i].Item1, endpoints[i].Item2, clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, logger: logger);
                }
            }
            else
            {
                clients[0] = new GarnetClient(hostAddress, hostPort, clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, logger: logger);
            }
        }

        // Timeout deadline
        this.failoverTimeout = failoverTimeout == default ? TimeSpan.FromSeconds(600) : failoverTimeout;
        failoverDeadline = DateTime.UtcNow.Add(failoverTimeout);
        status = FailoverStatus.BEGIN_FAILOVER;
    }

    public void Dispose()
    {
        cts.Cancel();
        cts.Dispose();
        DisposeConnections();
    }

    private void DisposeConnections()
    {
        if (clients != null)
            foreach (GarnetClient client in clients)
                client?.Dispose();
    }
}