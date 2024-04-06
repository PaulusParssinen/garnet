﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        // Must be the same as the TsavoriteLog start address of allocator
        public static readonly long kFirstValidAofAddress = 64;
        readonly AofTaskStore aofTaskStore;

        public int ConnectedReplicasCount => aofTaskStore.CountConnectedReplicas();

        public List<(string, string)> GetReplicaInfo() => aofTaskStore.GetReplicaInfo(ReplicationOffset);

        public bool TryAddReplicationTask(string nodeid, long startAddress, out AofSyncTaskInfo aofSyncTaskInfo)
            => aofTaskStore.TryAddReplicationTask(nodeid, startAddress, out aofSyncTaskInfo);

        public long AofTruncatedUntil => aofTaskStore.AofTruncatedUntil;

        public bool TryRemoveReplicationTask(AofSyncTaskInfo aofSyncTaskInfo)
            => aofTaskStore.TryRemove(aofSyncTaskInfo);

        /// <summary>
        /// Safely truncate iterator
        /// </summary>
        /// <param name="CheckpointCoveredAofAddress"></param>
        /// <returns></returns>
        public long SafeTruncateAof(long CheckpointCoveredAofAddress)
            => aofTaskStore.SafeTruncateAof(CheckpointCoveredAofAddress);

        /// <summary>
        /// Try to initiate connection from primary to replica in order to stream aof.
        /// </summary>
        /// <param name="nodeid"></param>
        /// <param name="startAddress"></param>
        /// <param name="aofSyncTaskInfo"></param>
        /// <param name="errorMessage">The ASCII encoded error message if the method return <c>false</c>; otherwise <c>default</c></param>
        /// <returns></returns>
        public bool TryConnectToReplica(string nodeid, long startAddress, AofSyncTaskInfo aofSyncTaskInfo, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            if (_disposed)
            {
                aofTaskStore.TryRemove(aofSyncTaskInfo);

                errorMessage = "Replication Manager Disposed"u8;
                logger?.LogError(Encoding.ASCII.GetString(errorMessage));
                return false;
            }

            // TODO: why do we need to verify this?
            // No guarantee at call time that provided nodeId is of a trusted node because of gossip propagation delay
            var (address, port) = clusterProvider.clusterManager.CurrentConfig.GetWorkerAddressFromNodeId(nodeid);
            if (address == null)
            {
                aofTaskStore.TryRemove(aofSyncTaskInfo);
                var msg = $"unknown endpoint for {nodeid}";
                logger.LogError(msg);
                errorMessage = Encoding.ASCII.GetBytes(msg);
                return false;
            }

            var tailAddress = storeWrapper.appendOnlyFile.TailAddress;
            // Check if requested AOF address goes beyond the maximum available AOF address of this primary
            if (startAddress > storeWrapper.appendOnlyFile.TailAddress)
            {
                if (clusterProvider.serverOptions.MainMemoryReplication)
                {
                    logger?.LogWarning("MainMemoryReplication: Requested address {startAddress} unavailable. Local primary tail address {tailAddress}. Proceeding as best effort.", startAddress, tailAddress);
                }
                else
                {
                    aofTaskStore.TryRemove(aofSyncTaskInfo);
                    logger?.LogError("AOF sync task failed to start. Requested address {startAddress} unavailable. Local primary tail address {tailAddress}", startAddress, tailAddress);
                    errorMessage = Encoding.ASCII.GetBytes($"requested AOF address: {startAddress} goes beyond, primary tail address: {tailAddress}");
                    return false;
                }
            }

            Task.Run(aofSyncTaskInfo.ReplicaSyncTask);
            return true;
        }
    }
}