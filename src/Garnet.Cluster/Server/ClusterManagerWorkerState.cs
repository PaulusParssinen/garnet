// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.Common;
using Microsoft.Extensions.Logging;

namespace Garnet.Cluster;

/// <summary>
/// Cluster manager
/// </summary>
internal sealed partial class ClusterManager : IDisposable
{
    /// <summary>
    /// Add worker with specified slots
    /// Update existing only if new config epoch is larger or current config epoch is zero
    /// </summary>
    public bool TryInitializeLocalWorker(
        string nodeId,
        string address,
        int port,
        long configEpoch,
        long currentConfigEpoch,
        long lastVotedConfigEpoch,
        NodeRole role,
        string replicaOfNodeId,
        string hostname)
    {
        while (true)
        {
            ClusterConfig current = currentConfig;
            ClusterConfig newConfig = current.InitializeLocalWorker(nodeId, address, port, configEpoch, currentConfigEpoch, lastVotedConfigEpoch, role, replicaOfNodeId, hostname);
            if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                break;
        }
        FlushConfig();
        return true;
    }

    /// <summary>
    /// Try remove worker through the forget command.
    /// </summary>
    /// <param name="nodeid"></param>
    /// <param name="expirySeconds"></param>
    /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
    public bool TryRemoveWorker(string nodeid, int expirySeconds, out ReadOnlySpan<byte> errorMessage)
    {
        try
        {
            PauseConfigMerge();
            errorMessage = default;
            while (true)
            {
                ClusterConfig current = currentConfig;

                if (current.LocalNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_FORGET_MYSELF;
                    return false;
                }

                if (current.GetNodeRoleFromNodeId(nodeid) == NodeRole.UNASSIGNED)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR I don't know about node {nodeid}.");
                    return false;
                }

                if (current.LocalNodeRole == NodeRole.REPLICA && current.LocalNodePrimaryId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_FORGET_MY_PRIMARY;
                    return false;
                }

                ClusterConfig newConfig = current.RemoveWorker(nodeid);
                long expiry = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expirySeconds).Ticks;
                _ = workerBanList.AddOrUpdate(nodeid, expiry, (key, oldValue) => expiry);
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            return true;
        }
        finally
        {
            UnpauseConfigMerge();
        }
    }

    /// <summary>
    /// Reset cluster config and generated new node id if HARD reset specified
    /// </summary>
    /// <param name="soft"></param>
    /// <param name="expirySeconds"></param>
    /// <returns></returns>
    public ReadOnlySpan<byte> TryReset(bool soft, int expirySeconds = 60)
    {
        try
        {
            PauseConfigMerge();
            ReadOnlySpan<byte> resp = CmdStrings.RESP_OK;

            while (true)
            {
                ClusterConfig current = currentConfig;
                string newNodeId = soft ? current.LocalNodeId : Generator.CreateHexId();
                string address = current.LocalNodeIp;
                int port = current.LocalNodePort;

                long configEpoch = soft ? current.LocalNodeConfigEpoch : 0;
                long currentConfigEpoch = soft ? current.LocalNodeCurrentConfigEpoch : 0;
                long lastVotedConfigEpoch = soft ? current.LocalNodeLastVotedEpoch : 0;

                long expiry = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expirySeconds).Ticks;
                foreach (string nodeId in current.GetRemoteNodeIds())
                    _ = workerBanList.AddOrUpdate(nodeId, expiry, (key, oldValue) => expiry);

                ClusterConfig newConfig = new ClusterConfig().InitializeLocalWorker(
                    newNodeId,
                    address,
                    port,
                    configEpoch: configEpoch,
                    currentConfigEpoch: currentConfigEpoch,
                    lastVotedConfigEpoch: lastVotedConfigEpoch,
                    role: NodeRole.PRIMARY,
                    replicaOfNodeId: null,
                    Format.GetHostName());
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            return resp;
        }
        finally
        {
            UnpauseConfigMerge();
        }
    }

    /// <summary>
    /// Try to make this node a replica of node with nodeid
    /// </summary>
    /// <param name="nodeid"></param>
    /// <param name="force">Check if node is clean (i.e. is PRIMARY without any assigned nodes)</param>
    /// <param name="recovering"></param>
    /// <param name="errorMessage">The ASCII encoded error response if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
    /// <param name="logger"></param>
    public bool TryAddReplica(string nodeid, bool force, ref bool recovering, out ReadOnlySpan<byte> errorMessage, ILogger logger = null)
    {
        errorMessage = default;
        while (true)
        {
            ClusterConfig current = CurrentConfig;
            if (current.LocalNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_REPLICATE_SELF;
                logger?.LogError(Encoding.ASCII.GetString(errorMessage));
                return false;
            }

            if (!force && current.LocalNodeRole != NodeRole.PRIMARY)
            {
                logger?.LogError("ERR I am already replica of {localNodePrimaryId}", current.LocalNodePrimaryId);
                errorMessage = Encoding.ASCII.GetBytes($"ERR I am already replica of {current.LocalNodePrimaryId}.");
                return false;
            }

            if (!force && current.HasAssignedSlots(1))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_MAKE_REPLICA_WITH_ASSIGNED_SLOTS;
                logger?.LogError(Encoding.ASCII.GetString(errorMessage));
                return false;
            }

            int workerId = current.GetWorkerIdFromNodeId(nodeid);
            if (workerId == 0)
            {
                errorMessage = Encoding.ASCII.GetBytes($"ERR I don't know about node {nodeid}.");
                logger?.LogError("ERR I don't know about node {nodeid}.", nodeid);
                return false;
            }

            if (current.GetNodeRoleFromNodeId(nodeid) != NodeRole.PRIMARY)
            {
                logger?.LogError("ERR Trying to replicate node ({nodeid}) that is not a primary.", nodeid);
                errorMessage = Encoding.ASCII.GetBytes($"ERR Trying to replicate node ({nodeid}) that is not a primary.");
                return false;
            }

            recovering = true;
            ClusterConfig newConfig = currentConfig.MakeReplicaOf(nodeid);
            newConfig = newConfig.BumpLocalNodeConfigEpoch();
            if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                break;
        }
        FlushConfig();
        return true;
    }

    /// <summary>
    /// List replicas of specified primary with given nodeid
    /// </summary>
    /// <param name="nodeid"></param>
    public List<string> ListReplicas(string nodeid)
    {
        ClusterConfig current = CurrentConfig;
        return current.GetReplicas(nodeid);
    }
}