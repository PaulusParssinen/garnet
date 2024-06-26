﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;
using Garnet.Networking;
using Garnet.Server.ACL;
using Garnet.Server.Auth;
using Microsoft.Extensions.Logging;
using Tsavorite;

namespace Garnet.Server;

using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

/// <summary>
/// Cluster provider
/// </summary>
public interface IClusterProvider : IDisposable
{
    /// <summary>
    /// Create cluster session
    /// </summary>
    IClusterSession CreateClusterSession(TransactionManager txnManager, IGarnetAuthenticator authenticator, User user, GarnetSessionMetrics garnetSessionMetrics, BasicGarnetApi basicGarnetApi, INetworkSender networkSender, ILogger logger = null);

    /// <summary>
    /// Flush config
    /// </summary>
    void FlushConfig();

    /// <summary>
    /// Get gossip stats
    /// </summary>
    MetricsItem[] GetGossipStats(bool metricsDisabled);

    /// <summary>
    /// Get replication info
    /// </summary>
    MetricsItem[] GetReplicationInfo();

    /// <summary>
    /// Is replica
    /// </summary>
    bool IsReplica();

    /// <summary>
    /// On checkpoint initiated
    /// </summary>
    void OnCheckpointInitiated(out long CheckpointCoveredAofAddress);

    /// <summary>
    /// Recover the cluster
    /// </summary>
    void Recover();

    /// <summary>
    /// Reset gossip stats
    /// </summary>
    void ResetGossipStats();

    /// <summary>
    /// Safe truncate AOF
    /// </summary>
    void SafeTruncateAOF(StoreType storeType, bool full, long CheckpointCoveredAofAddress, Guid storeCheckpointToken, Guid objectStoreCheckpointToken);

    /// <summary>
    /// Start cluster operations
    /// </summary>
    void Start();

    /// <summary>
    /// Update cluster auth (atomically)
    /// </summary>
    void UpdateClusterAuth(string clusterUsername, string clusterPassword);
}