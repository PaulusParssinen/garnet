﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using System.Text;
using Garnet.Common;
using Garnet.Server;
using Microsoft.Extensions.Logging;
using Tsavorite;

namespace Garnet.Cluster;

/// <summary>
/// Server session for RESP protocol - cluster commands are in this file
/// </summary>
internal sealed unsafe partial class ClusterSession : IClusterSession
{
    private ClusterConfig lastSentConfig;
    private int migrateSetCount = 0;
    private byte migrateState = 0;

    private int CountKeysInSessionStore(int slot)
    {
        ClusterKeyIterationFunctions.MainStoreCountKeys iterFuncs = new(slot);
        _ = basicGarnetApi.IterateMainStore(ref iterFuncs);
        return iterFuncs.keyCount;
    }

    private int CountKeysInObjectStore(int slot)
    {
        if (!clusterProvider.serverOptions.DisableObjects)
        {
            ClusterKeyIterationFunctions.ObjectStoreCountKeys iterFuncs = new(slot);
            _ = basicGarnetApi.IterateObjectStore(ref iterFuncs);
            return iterFuncs.keyCount;
        }
        return 0;
    }

    private int CountKeysInSlot(int slot) => CountKeysInSessionStore(slot) + CountKeysInObjectStore(slot);

    private List<byte[]> GetKeysInSlot(int slot, int keyCount)
    {
        List<byte[]> keys = [];
        ClusterKeyIterationFunctions.MainStoreGetKeysInSlot mainIterFuncs = new(keys, slot, keyCount);
        _ = basicGarnetApi.IterateMainStore(ref mainIterFuncs);

        if (!clusterProvider.serverOptions.DisableObjects)
        {
            ClusterKeyIterationFunctions.ObjectStoreGetKeysInSlot objectIterFuncs = new(keys, slot);
            _ = basicGarnetApi.IterateObjectStore(ref objectIterFuncs);
        }
        return keys;
    }

    /// <summary>
    /// Try to parse slots
    /// </summary>
    /// <param name="errorMessage">
    /// The ASCII encoded error message if there one of the following conditions is true
    /// <list type="bullet">
    ///   <item>If the same slot is specified multiple times.</item>
    ///   <item>If the slot is out of range.</item>
    /// </list>
    /// otherwise <see langword="default" />
    /// </param>
    /// <returns>A boolean indicating that there was error in parsing of the arguments.</returns>
    /// <remarks>
    /// The error handling is little special for this method because we need to drain all arguments even in the case of error.
    /// <para/>
    /// The <paramref name="errorMessage"/> will only have a generic error message set in the event of duplicate or out of range slot. 
    /// The method will still return <see langword="true" /> in case of such error.
    /// </remarks>
    private bool TryParseSlots(int count, ref byte* ptr, out HashSet<int> slots, out ReadOnlySpan<byte> errorMessage, bool range)
    {
        slots = [];
        errorMessage = default;
        bool duplicate = false;
        bool outOfRange = false;
        bool invalidRange = false;
        int slotStart;
        int slotEnd;

        while (count > 0)
        {
            if (range)
            {
                if (!RespReadUtils.ReadIntWithLengthHeader(out slotStart, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadIntWithLengthHeader(out slotEnd, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                count -= 2;
            }
            else
            {
                if (!RespReadUtils.ReadIntWithLengthHeader(out slotStart, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                count--;
                slotEnd = slotStart;
            }

            if (duplicate || outOfRange || invalidRange)
                continue;

            if (slotStart > slotEnd)
            {
                invalidRange = true;
                continue;
            }

            if (ClusterConfig.OutOfRange(slotStart) || ClusterConfig.OutOfRange(slotEnd))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE;
                outOfRange = true;
            }

            for (int slot = slotStart; slot <= slotEnd && !duplicate; slot++)
            {
                if (!slots.Add(slot))
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Slot {slot} specified multiple times");
                    duplicate = true;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Release epoch, wait for config transition and re-acquire the epoch
    /// </summary>
    public void UnsafeWaitForConfigTransition()
    {
        ReleaseCurrentEpoch();
        clusterProvider.WaitForConfigTransition();
        AcquireCurrentEpoch();
    }

    private bool ProcessClusterCommands(ReadOnlySpan<byte> bufSpan, int count)
    {
        if (clusterProvider.clusterManager == null)
        {
            if (!DrainCommands(bufSpan, count))
                return false;
            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CLUSTER, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        bool errorFlag;
        string errorCmd;
        if (count > 0)
        {
            ReadOnlySpan<byte> param = GetCommand(bufSpan, out bool success1);
            if (!success1) return false;

            if (ProcessClusterBasicCommands(bufSpan, param, count - 1, out errorFlag, out errorCmd))
                goto checkErrorFlags;
            else if (ProcessFailoverCommands(bufSpan, param, count - 1, out errorFlag, out errorCmd))
                goto checkErrorFlags;
            else if (ProcessSlotManageCommands(bufSpan, param, count - 1, out errorFlag, out errorCmd))
                goto checkErrorFlags;
            else if (ProcessClusterMigrationCommands(bufSpan, param, count - 1, out errorFlag, out errorCmd))
                goto checkErrorFlags;
            else if (ProcessClusterReplicationCommands(bufSpan, param, count - 1, out errorFlag, out errorCmd))
                goto checkErrorFlags;
            else
            {
                if (!DrainCommands(bufSpan, count - 1))
                    return false;
                string paramStr = Encoding.ASCII.GetString(param);
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for '{paramStr}'. Try CLUSTER HELP.", ref dcurr, dend))
                    SendAndReset();
            }
        }
        else
        {
            errorFlag = true;
            errorCmd = "CLUSTER";
        }

    checkErrorFlags:
        if (errorFlag && !string.IsNullOrWhiteSpace(errorCmd))
        {
            string errorMsg = string.Format(CmdStrings.GenericErrMissingParam, errorCmd);
            byte[] bresp_ERRMISSINGPARAM = Encoding.ASCII.GetBytes(errorMsg);
            bresp_ERRMISSINGPARAM.CopyTo(new Span<byte>(dcurr, bresp_ERRMISSINGPARAM.Length));
            dcurr += bresp_ERRMISSINGPARAM.Length;
        }
        sessionMetrics?.incr_total_cluster_commands_processed();
        return true;
    }

    private bool ProcessClusterBasicCommands(ReadOnlySpan<byte> bufSpan, ReadOnlySpan<byte> param, int count, out bool errorFlag, out string errorCmd)
    {
        errorFlag = false;
        errorCmd = string.Empty;
        if (param.SequenceEqual(CmdStrings.BUMPEPOCH) || param.SequenceEqual(CmdStrings.bumpepoch))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count > 0)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                success = clusterProvider.clusterManager.TryBumpClusterEpoch();
                readHead = (int)(ptr - recvBufferPtr);

                if (success)
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CONFIG_UPDATE, ref dcurr, dend))
                        SendAndReset();
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.FORGET) || param.SequenceEqual(CmdStrings.forget))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count < 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadStringWithLengthHeader(out string nodeid, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                int expirySeconds = 60;
                if (count == 2)
                {
                    if (!RespReadUtils.ReadIntWithLengthHeader(out expirySeconds, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                }
                readHead = (int)(ptr - recvBufferPtr);

                logger?.LogTrace("CLUSTER FORGET {nodeid} {seconds}", nodeid, expirySeconds);
                if (!clusterProvider.clusterManager.TryRemoveWorker(nodeid, expirySeconds, out ReadOnlySpan<byte> errorMessage))
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.INFO) || param.SequenceEqual(CmdStrings.info))
        {
            if (count > 0)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                readHead = (int)(ptr - recvBufferPtr);
                string clusterInfo = clusterProvider.clusterManager.GetInfo();
                while (!RespWriteUtils.WriteAsciiBulkString(clusterInfo, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.HELP) || param.SequenceEqual(CmdStrings.help))
        {
            byte* ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            List<string> clusterCommands = ClusterCommandInfo.GetClusterCommands();
            while (!RespWriteUtils.WriteArrayLength(clusterCommands.Count, ref dcurr, dend))
                SendAndReset();
            foreach (string command in clusterCommands)
            {
                while (!RespWriteUtils.WriteSimpleString(command, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.MEET) || param.SequenceEqual(CmdStrings.meet))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count != 2)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] ipaddress, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadIntWithLengthHeader(out int port, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                string ipaddressStr = Encoding.ASCII.GetString(ipaddress);
                logger?.LogTrace("CLUSTER MEET {ipaddressStr} {port}", ipaddressStr, port);
                clusterProvider.clusterManager.RunMeetTask(ipaddressStr, port);
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.MYID) || param.SequenceEqual(CmdStrings.myid))
        {
            byte* ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            while (!RespWriteUtils.WriteAsciiBulkString(clusterProvider.clusterManager.CurrentConfig.LocalNodeId, ref dcurr, dend))
                SendAndReset();
        }
        else if (param.SequenceEqual(CmdStrings.MYPARENTID) || param.SequenceEqual(CmdStrings.myparentid))
        {
            byte* ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);

            ClusterConfig current = clusterProvider.clusterManager.CurrentConfig;
            string parentId = current.LocalNodeRole == NodeRole.PRIMARY ? current.LocalNodeId : current.LocalNodePrimaryId;
            while (!RespWriteUtils.WriteAsciiBulkString(parentId, ref dcurr, dend))
                SendAndReset();
        }
        else if (param.SequenceEqual(CmdStrings.ENDPOINT) || param.SequenceEqual(CmdStrings.endpoint))
        {
            byte* ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out string nodeid, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);
            ClusterConfig current = clusterProvider.clusterManager.CurrentConfig;
            (string host, int port) = current.GetEndpointFromNodeId(nodeid);
            while (!RespWriteUtils.WriteAsciiBulkString($"{host}:{port}", ref dcurr, dend))
                SendAndReset();
        }
        else if (param.SequenceEqual(CmdStrings.NODES) || param.SequenceEqual(CmdStrings.nodes))
        {
            if (count > 0)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                readHead = (int)(ptr - recvBufferPtr);
                string nodes = clusterProvider.clusterManager.CurrentConfig.GetClusterInfo();
                while (!RespWriteUtils.WriteAsciiBulkString(nodes, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.set_config_epoch) || param.SequenceEqual(CmdStrings.SET_CONFIG_EPOCH))
        {
            if (count != 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                Debug.WriteLine($"{Encoding.UTF8.GetString(new Span<byte>(recvBufferPtr, Math.Min(bytesRead, 128))).Replace("\n", "|").Replace("\r", "")}");
                byte* ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadIntWithLengthHeader(out int configEpoch, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);
                if (clusterProvider.clusterManager.CurrentConfig.NumWorkers > 2)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CONFIG_EPOCH_ASSIGNMENT, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    if (!clusterProvider.clusterManager.TrySetLocalConfigEpoch(configEpoch, out ReadOnlySpan<byte> errorMessage))
                    {
                        while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.SHARDS) || param.SequenceEqual(CmdStrings.shards))
        {
            byte* ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            string shardsInfo = clusterProvider.clusterManager.CurrentConfig.GetShardsInfo();
            while (!RespWriteUtils.WriteAsciiDirect(shardsInfo, ref dcurr, dend))
                SendAndReset();
        }
        else if (param.SequenceEqual(CmdStrings.GOSSIP))
        {
            if (count < 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                bool gossipWithMeet = false;
                if (count > 1)
                {
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] withMeet, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    Debug.Assert(withMeet.SequenceEqual(CmdStrings.WITHMEET.ToArray()));
                    if (withMeet.SequenceEqual(CmdStrings.WITHMEET.ToArray()))
                        gossipWithMeet = true;
                }

                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] gossipMessage, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                clusterProvider.clusterManager.gossipStats.UpdateGossipBytesRecv(gossipMessage.Length);
                ClusterConfig current = clusterProvider.clusterManager.CurrentConfig;

                // Try merge if not just a ping message
                if (gossipMessage.Length > 0)
                {
                    var other = ClusterConfig.FromByteArray(gossipMessage);
                    // Accept gossip message if it is a gossipWithMeet or node from node that is already known and trusted
                    // GossipWithMeet messages are only send through a call to CLUSTER MEET at the remote node
                    if (gossipWithMeet || current.IsKnown(other.LocalNodeId))
                    {
                        _ = clusterProvider.clusterManager.TryMerge(other);
                    }
                    else
                        logger?.LogWarning("Received gossip from unknown node: {node-id}", other.LocalNodeId);
                }

                // Respond if configuration has changed or gossipWithMeet option is specified
                if (lastSentConfig != current || gossipWithMeet)
                {
                    byte[] configByteArray = current.ToByteArray();
                    clusterProvider.clusterManager.gossipStats.UpdateGossipBytesSend(configByteArray.Length);
                    while (!RespWriteUtils.WriteBulkString(configByteArray, ref dcurr, dend))
                        SendAndReset();
                    lastSentConfig = current;
                }
                else
                {
                    while (!RespWriteUtils.WriteBulkString(Array.Empty<byte>(), ref dcurr, dend))
                        SendAndReset();
                }
                return true;
            }
        }
        else if (param.SequenceEqual(CmdStrings.RESET) || param.SequenceEqual(CmdStrings.reset))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            byte* ptr = recvBufferPtr + readHead;
            bool soft = true;
            int expirySeconds = 60;

            if (count > 0)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out string option, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (option.Equals("HARD", StringComparison.OrdinalIgnoreCase))
                    soft = false;
            }

            if (count > 1)
            {
                if (!RespReadUtils.ReadIntWithLengthHeader(out expirySeconds, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            readHead = (int)(ptr - recvBufferPtr);

            clusterProvider.clusterManager.TryReset(soft, expirySeconds);
            if (!soft) clusterProvider.FlushDB(true);

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
        }
        else { return false; }
        return true;
    }

    public bool ProcessFailoverCommands(ReadOnlySpan<byte> bufSpan, ReadOnlySpan<byte> param, int count, out bool errorFlag, out string errorCmd)
    {
        errorFlag = false;
        errorCmd = string.Empty;
        if (param.SequenceEqual(CmdStrings.FAILOVER) || param.SequenceEqual(CmdStrings.failover))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count < 0)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                FailoverOption failoverOption = FailoverOption.DEFAULT;
                TimeSpan failoverTimeout = default;
                if (count > 0)
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out string failoverOptionStr, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    if (!Enum.TryParse(failoverOptionStr, ignoreCase: true, out failoverOption))
                    {
                        while (!RespWriteUtils.WriteError($"ERR Failover option ({failoverOptionStr}) not supported", ref dcurr, dend))
                            SendAndReset();
                        failoverOption = FailoverOption.INVALID;
                    }
                }

                if (count > 1)
                {
                    if (!RespReadUtils.ReadIntWithLengthHeader(out int failoverTimeoutSeconds, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    failoverTimeout = TimeSpan.FromSeconds(failoverTimeoutSeconds);
                }
                readHead = (int)(ptr - recvBufferPtr);

                if (clusterProvider.serverOptions.EnableAOF)
                {
                    if (failoverOption == FailoverOption.ABORT)
                    {
                        clusterProvider.failoverManager.TryAbortReplicaFailover();
                    }
                    else
                    {
                        ClusterConfig current = clusterProvider.clusterManager.CurrentConfig;
                        NodeRole nodeRole = current.LocalNodeRole;
                        if (nodeRole == NodeRole.REPLICA)
                        {
                            if (!clusterProvider.failoverManager.TryStartReplicaFailover(failoverOption, failoverTimeout))
                            {
                                while (!RespWriteUtils.WriteError($"ERR failed to start failover for primary({current.GetLocalNodePrimaryAddress()})", ref dcurr, dend))
                                    SendAndReset();
                                return true;
                            }
                        }
                        else
                        {
                            while (!RespWriteUtils.WriteError($"ERR Node is not a {NodeRole.REPLICA} ~{nodeRole}~", ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }
                    }
                }
                else
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.failauthreq))
        {
            byte* ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] nodeIdBytes, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] requestEpochBytes, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] claimedSlots, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            ReadOnlySpan<byte> resp = clusterProvider.clusterManager.AuthorizeFailover(
                Encoding.ASCII.GetString(nodeIdBytes),
                BitConverter.ToInt64(requestEpochBytes),
                claimedSlots) ? CmdStrings.RESP_RETURN_VAL_1 : CmdStrings.RESP_RETURN_VAL_0;
            while (!RespWriteUtils.WriteDirect(resp, ref dcurr, dend))
                SendAndReset();
        }
        else if (param.SequenceEqual(CmdStrings.failstopwrites))
        {
            byte* ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] nodeIdBytes, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);
            clusterProvider.clusterManager.TryStopWrites(Encoding.ASCII.GetString(nodeIdBytes));
            UnsafeWaitForConfigTransition();
            while (!RespWriteUtils.WriteInteger(clusterProvider.replicationManager.ReplicationOffset, ref dcurr, dend))
                SendAndReset();
        }
        else if (param.SequenceEqual(CmdStrings.failreplicationoffset))
        {
            byte* ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadLongWithLengthHeader(out long primaryReplicationOffset, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            long rOffset = clusterProvider.replicationManager.WaitForReplicationOffset(primaryReplicationOffset).GetAwaiter().GetResult();
            while (!RespWriteUtils.WriteInteger(rOffset, ref dcurr, dend))
                SendAndReset();
        }
        else { return false; }
        return true;
    }

    public bool ProcessSlotManageCommands(ReadOnlySpan<byte> bufSpan, ReadOnlySpan<byte> param, int count, out bool errorFlag, out string errorCmd)
    {
        errorFlag = false;
        errorCmd = string.Empty;
        if (param.SequenceEqual(CmdStrings.ADDSLOTS) || param.SequenceEqual(CmdStrings.addslots))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count < 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;

                //Try to parse slot ranges.
                bool slotsParsed = TryParseSlots(count, ref ptr, out HashSet<int> slots, out ReadOnlySpan<byte> errorMessage, range: false);

                readHead = (int)(ptr - recvBufferPtr);

                //The slot parsing may give errorMessage even if the methods TryParseSlots true.
                if (slotsParsed && errorMessage != default)
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                else if (!slotsParsed) return false;

                //Try to to add slots
                if (!clusterProvider.clusterManager.TryAddSlots(slots, out int slotIndex) &&
                    slotIndex != -1)
                {
                    while (!RespWriteUtils.WriteError($"ERR Slot {slotIndex} is already busy", ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.ADDSLOTSRANGE) || param.SequenceEqual(CmdStrings.addslotsrange))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count < 2 || (count & 0x1) == 0x1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;

                // Try to parse slot ranges.
                bool slotsParsed = TryParseSlots(count, ref ptr, out HashSet<int> slots, out ReadOnlySpan<byte> errorMessage, range: true);

                readHead = (int)(ptr - recvBufferPtr);

                //The slot parsing may give errorMessage even if the TryParseSlots returns true.
                if (slotsParsed && errorMessage != default)
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                else if (!slotsParsed) return false;

                // Try to to add slots
                if (!clusterProvider.clusterManager.TryAddSlots(slots, out int slotIndex) &&
                    slotIndex != -1)
                {
                    while (!RespWriteUtils.WriteError($"ERR Slot {slotIndex} is already busy", ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.BANLIST) || param.SequenceEqual(CmdStrings.banlist))
        {
            byte* ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            List<string> banlist = clusterProvider.clusterManager.GetBanList();

            while (!RespWriteUtils.WriteArrayLength(banlist.Count, ref dcurr, dend))
                SendAndReset();
            foreach (string replica in banlist)
            {
                while (!RespWriteUtils.WriteAsciiBulkString(replica, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.COUNTKEYSINSLOT) || param.SequenceEqual(CmdStrings.countkeysinslot))
        {
            ClusterConfig current = clusterProvider.clusterManager.CurrentConfig;
            if (count != 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                if (ClusterConfig.OutOfRange(slot))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
                        SendAndReset();
                }
                else if (!current.IsLocal((ushort)slot))
                {
                    Redirect((ushort)slot, current);
                }
                else
                {
                    try
                    {
                        int keyCount = CountKeysInSlot(slot);
                        while (!RespWriteUtils.WriteInteger(keyCount, ref dcurr, dend))
                            SendAndReset();
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Critical error in count keys");
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_N1, ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.DELSLOTS) || param.SequenceEqual(CmdStrings.delslots))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count < 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                //Try to parse slot ranges.
                bool slotsParsed = TryParseSlots(count, ref ptr, out HashSet<int> slots, out ReadOnlySpan<byte> errorMessage, range: false);

                readHead = (int)(ptr - recvBufferPtr);

                //The slot parsing may give errorMessage even if the TryParseSlots returns true.
                if (slotsParsed && errorMessage != default)
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                else if (!slotsParsed) return false;

                //Try remove the slots
                if (!clusterProvider.clusterManager.TryRemoveSlots(slots, out int slotIndex) &&
                    slotIndex != -1)
                {
                    while (!RespWriteUtils.WriteError($"ERR Slot {slotIndex} is already not assigned", ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.DELSLOTSRANGE) || param.SequenceEqual(CmdStrings.delslotsrange))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            // CLUSTER ADDSLOTSRANGE [start-slot end-slot] // 2 + [2] even number of arguments
            if (count < 2 || (count & 0x1) == 0x1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;

                //Try to parse slot ranges.
                bool slotsParsed = TryParseSlots(count, ref ptr, out HashSet<int> slots, out ReadOnlySpan<byte> errorMessage, range: true);

                readHead = (int)(ptr - recvBufferPtr);

                //The slot parsing may give errorMessage even if the TryParseSlots returns true.
                if (slotsParsed && errorMessage != default)
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                else if (!slotsParsed) return false;

                //Try remove the slots
                if (!clusterProvider.clusterManager.TryRemoveSlots(slots, out int slotIndex) &&
                    slotIndex != -1)
                {
                    while (!RespWriteUtils.WriteError($"ERR Slot {slotIndex} is already not assigned", ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.DELKEYSINSLOT) || param.SequenceEqual(CmdStrings.delkeysinslot))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count != 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                readHead = (int)(ptr - recvBufferPtr);

                var slots = new HashSet<int>() { slot };
                ClusterManager.DeleteKeysInSlotsFromMainStore(basicGarnetApi, slots);
                if (!clusterProvider.serverOptions.DisableObjects)
                    ClusterManager.DeleteKeysInSlotsFromObjectStore(basicGarnetApi, slots);

                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.DELKEYSINSLOTRANGE) || param.SequenceEqual(CmdStrings.delkeysinslotrange))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count != 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;

                //Try to parse slot ranges.
                bool slotsParsed = TryParseSlots(count, ref ptr, out HashSet<int> slots, out ReadOnlySpan<byte> errorMessage, range: true);

                readHead = (int)(ptr - recvBufferPtr);

                //The slot parsing may give errorMessage even if the TryParseSlots returns true.
                if (slotsParsed && errorMessage != default)
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                else if (!slotsParsed) return false;

                ClusterManager.DeleteKeysInSlotsFromMainStore(basicGarnetApi, slots);
                if (!clusterProvider.serverOptions.DisableObjects)
                    ClusterManager.DeleteKeysInSlotsFromObjectStore(basicGarnetApi, slots);

                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.GETKEYSINSLOT) || param.SequenceEqual(CmdStrings.getkeysinslot))
        {
            ClusterConfig current = clusterProvider.clusterManager.CurrentConfig;

            if (count < 2)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadIntWithLengthHeader(out int keyCount, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                readHead = (int)(ptr - recvBufferPtr);

                if (ClusterConfig.OutOfRange(slot))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
                        SendAndReset();
                }
                else if (!current.IsLocal((ushort)slot))
                {
                    Redirect((ushort)slot, current);
                }
                else
                {
                    List<byte[]> keys = GetKeysInSlot(slot, keyCount);
                    int keyCountRet = Math.Min(keys.Count, keyCount);
                    while (!RespWriteUtils.WriteArrayLength(keyCountRet, ref dcurr, dend))
                        SendAndReset();
                    for (int i = 0; i < keyCountRet; i++)
                        while (!RespWriteUtils.WriteBulkString(keys[i], ref dcurr, dend))
                            SendAndReset();
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.KEYSLOT) || param.SequenceEqual(CmdStrings.keyslot))
        {
            if (count < 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                byte* keyPtr = null;
                int ksize = 0;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                int slot = NumUtils.HashSlot(keyPtr, ksize);
                while (!RespWriteUtils.WriteInteger(slot, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.SETSLOT) || param.SequenceEqual(CmdStrings.setslot))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count < 2)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadStringWithLengthHeader(out string subcommand, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!Enum.TryParse(subcommand, ignoreCase: true, out SlotState slotState))
                    slotState = SlotState.STABLE;

                string nodeid = null;
                if (count > 2)
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out nodeid, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                }
                readHead = (int)(ptr - recvBufferPtr);

                if (!ClusterConfig.OutOfRange(slot))
                {
                    // Try to set slot state
                    bool setSlotsSucceeded;
                    ReadOnlySpan<byte> errorMessage = default;
                    switch (slotState)
                    {
                        case SlotState.STABLE:
                            setSlotsSucceeded = true;
                            clusterProvider.clusterManager.ResetSlotState(slot);
                            break;
                        case SlotState.IMPORTING:
                            setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotForImport(slot, nodeid, out errorMessage);
                            break;
                        case SlotState.MIGRATING:
                            setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotForMigration(slot, nodeid, out errorMessage);
                            break;
                        case SlotState.NODE:
                            setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotForOwnershipChange(slot, nodeid, out errorMessage);
                            break;
                        default:
                            setSlotsSucceeded = false;
                            errorMessage = Encoding.ASCII.GetBytes($"ERR Slot state {subcommand} not supported.");
                            break;
                    }

                    if (setSlotsSucceeded)
                    {
                        UnsafeWaitForConfigTransition();

                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
                        SendAndReset();
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.SETSLOTSRANGE) || param.SequenceEqual(CmdStrings.setslotsrange))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            if (count < 3)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                // CLUSTER SETSLOTRANGE IMPORTING <source-node-id> <slot-start> <slot-end> [slot-start slot-end]
                // CLUSTER SETSLOTRANGE MIGRATING <destination-node-id> <slot-start> <slot-end> [slot-start slot-end]
                // CLUSTER SETSLOTRANGE NODE <node-id> <slot-start> <slot-end> [slot-start slot-end]
                // CLUSTER SETSLOTRANGE STABLE <slot-start> <slot-end> [slot-start slot-end]

                string nodeid = default;
                int _count = count - 1;
                byte* ptr = recvBufferPtr + readHead;
                // Extract subcommand
                if (!RespReadUtils.ReadStringWithLengthHeader(out string subcommand, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Try parse slot state
                if (!Enum.TryParse(subcommand, out SlotState slotState))
                {
                    // Log error for invalid slot state option
                    logger?.LogError("The given input '{input}' is not a valid slot state option.", subcommand);
                    if (!DrainCommands(bufSpan, count - 1))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param);
                    return true;
                }

                // Extract nodeid for operations other than stable
                if (slotState != SlotState.STABLE)
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out nodeid, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    _count = count - 2;
                }

                // Try to parse slot ranges. The parsing may give errorMessage even if the TryParseSlots returns true.
                bool slotsParsed = TryParseSlots(_count, ref ptr, out HashSet<int> slots, out ReadOnlySpan<byte> errorMessage, range: true);
                if (slotsParsed && errorMessage != default)
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                else if (!slotsParsed) return false;

                readHead = (int)(ptr - recvBufferPtr);

                // Try to set slot states
                bool setSlotsSucceeded;
                switch (slotState)
                {
                    case SlotState.STABLE:
                        setSlotsSucceeded = true;
                        clusterProvider.clusterManager.ResetSlotsState(slots);
                        break;
                    case SlotState.IMPORTING:
                        setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotsForImport(slots, nodeid, out errorMessage);
                        break;
                    case SlotState.MIGRATING:
                        setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotsForMigration(slots, nodeid, out errorMessage);
                        break;
                    case SlotState.NODE:
                        setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotsForOwnershipChange(slots, nodeid, out errorMessage);
                        break;
                    default:
                        setSlotsSucceeded = false;
                        errorMessage = Encoding.ASCII.GetBytes($"ERR Slot state {subcommand} not supported.");
                        break;
                }

                if (setSlotsSucceeded)
                {
                    UnsafeWaitForConfigTransition();

                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.SLOTS) || param.SequenceEqual(CmdStrings.slots))
        {
            byte* ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            string slotsInfo = clusterProvider.clusterManager.CurrentConfig.GetSlotsInfo();
            while (!RespWriteUtils.WriteAsciiDirect(slotsInfo, ref dcurr, dend))
                SendAndReset();
        }
        else if (param.SequenceEqual(CmdStrings.SLOTSTATE) || param.SequenceEqual(CmdStrings.slotstate))
        {
            // CLUSTER SLOTSTATE <slot>
            if (count < 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                ClusterConfig current = clusterProvider.clusterManager.CurrentConfig;
                string nodeId = current.GetNodeIdFromSlot((ushort)slot);
                SlotState state = current.GetState((ushort)slot);
                string stateStr = state switch
                {
                    SlotState.STABLE => "=",
                    SlotState.IMPORTING => "<",
                    SlotState.MIGRATING => ">",
                    SlotState.OFFLINE => "-",
                    SlotState.FAIL => "-",
                    _ => throw new Exception($"Invalid SlotState filetype {state}"),
                };
                while (!RespWriteUtils.WriteAsciiDirect($"+{slot} {stateStr} {nodeId}\r\n", ref dcurr, dend))
                    SendAndReset();
            }
        }
        else { return false; }
        return true;
    }

    public bool ProcessClusterMigrationCommands(ReadOnlySpan<byte> bufSpan, ReadOnlySpan<byte> param, int count, out bool errorFlag, out string errorCmd)
    {
        errorFlag = false;
        errorCmd = string.Empty;

        if (param.SequenceEqual(CmdStrings.MIGRATE))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            // CLUSTER MIGRATE <node-id> <slot> <number-of-keys-in-slot> <serialized-data>
            if (count != 3)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                byte* ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadStringWithLengthHeader(out string sourceNodeId, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadStringWithLengthHeader(out string _replace, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadStringWithLengthHeader(out string storeType, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                bool replaceOption = _replace.Equals("T");

                // Check if payload size has been received
                if (ptr + 4 > recvBufferPtr + bytesRead)
                    return false;

                int headerLength = *(int*)ptr;
                ptr += 4;
                // Check if payload has been received
                if (ptr + headerLength > recvBufferPtr + bytesRead)
                    return false;

                ClusterConfig currentConfig = clusterProvider.clusterManager.CurrentConfig;

                if (storeType.Equals("SSTORE"))
                {
                    int keyCount = *(int*)ptr;
                    ptr += 4;
                    int i = 0;

                    while (i < keyCount)
                    {

                        byte* keyPtr = null, valPtr = null;
                        byte keyMetaDataSize = 0, valMetaDataSize = 0;
                        if (!RespReadUtils.ReadSerializedSpanByte(ref keyPtr, ref keyMetaDataSize, ref valPtr, ref valMetaDataSize, ref ptr, recvBufferPtr + bytesRead))
                            return false;

                        ref var key = ref SpanByte.Reinterpret(keyPtr);
                        if (keyMetaDataSize > 0) key.ExtraMetadata = *(long*)(keyPtr + 4);
                        ref var value = ref SpanByte.Reinterpret(valPtr);
                        if (valMetaDataSize > 0) value.ExtraMetadata = *(long*)(valPtr + 4);

                        // An error has occurred
                        if (migrateState > 0)
                        {
                            i++;
                            continue;
                        }

                        ushort slot = NumUtils.HashSlot(key.ToPointer(), key.LengthWithoutMetadata);
                        if (!currentConfig.IsImportingSlot(slot))//Slot is not in importing state
                        {
                            migrateState = 1;
                            i++;
                            continue;
                        }

                        if (i < migrateSetCount)
                            continue;

                        migrateSetCount++;

                        // Set if key replace flag is set or key does not exist
                        if (replaceOption || !CheckIfKeyExists(new ArgSlice(key.ToPointer(), key.Length)))
                            _ = basicGarnetApi.SET(ref key, ref value);
                        i++;
                    }
                }
                else if (storeType.Equals("OSTORE"))
                {
                    int keyCount = *(int*)ptr;
                    ptr += 4;
                    int i = 0;
                    while (i < keyCount)
                    {
                        if (!RespReadUtils.ReadSerializedData(out byte[] key, out byte[] data, out long expiration, ref ptr, recvBufferPtr + bytesRead))
                            return false;

                        // An error has occurred
                        if (migrateState > 0)
                            continue;

                        ushort slot = NumUtils.HashSlot(key);
                        if (!currentConfig.IsImportingSlot(slot))//Slot is not in importing state
                        {
                            migrateState = 1;
                            continue;
                        }

                        if (i < migrateSetCount)
                            continue;

                        migrateSetCount++;

                        IGarnetObject value = clusterProvider.storeWrapper.GarnetObjectSerializer.Deserialize(data);
                        value.Expiration = expiration;

                        // Set if key replace flag is set or key does not exist
                        if (replaceOption || !CheckIfKeyExists(key))
                            _ = basicGarnetApi.SET(key, value);

                        i++;
                    }
                }
                else
                {
                    throw new Exception("CLUSTER MIGRATE STORE TYPE ERROR!");
                }

                if (migrateState == 1)
                {
                    logger?.LogError("{errorMsg}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_NOT_IN_IMPORTING_STATE));
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_NOT_IN_IMPORTING_STATE, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }

                migrateSetCount = 0;
                migrateState = 0;
                readHead = (int)(ptr - recvBufferPtr);
            }
        }
        else if (param.SequenceEqual(CmdStrings.MTASKS))
        {
            if (count != 0)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                errorFlag = true;
                errorCmd = Encoding.ASCII.GetString(param);
            }
            else
            {
                int mtasks = clusterProvider.migrationManager.GetMigrationTaskCount();
                while (!RespWriteUtils.WriteInteger(mtasks, ref dcurr, dend))
                    SendAndReset();
                byte* ptr = recvBufferPtr + readHead;
                readHead = (int)(ptr - recvBufferPtr);
            }
        }
        else { return false; }
        return true;
    }

    private bool ProcessClusterReplicationCommands(ReadOnlySpan<byte> bufSpan, ReadOnlySpan<byte> param, int count, out bool errorFlag, out string errorCmd)
    {
        errorFlag = false;
        errorCmd = string.Empty;
        if (param.SequenceEqual(CmdStrings.REPLICAS) || param.SequenceEqual(CmdStrings.replicas))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            byte* ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out string nodeid, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);
            List<string> replicas = clusterProvider.clusterManager.ListReplicas(nodeid);

            while (!RespWriteUtils.WriteArrayLength(replicas.Count, ref dcurr, dend))
                SendAndReset();
            foreach (string replica in replicas)
            {
                while (!RespWriteUtils.WriteAsciiBulkString(replica, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.REPLICATE) || param.SequenceEqual(CmdStrings.replicate))
        {
            if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
            {
                return success;
            }

            byte* ptr = recvBufferPtr + readHead;
            bool background = false;
            if (!RespReadUtils.ReadStringWithLengthHeader(out string nodeid, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (count == 2)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out string backgroundFlag, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (backgroundFlag.Equals("SYNC", StringComparison.OrdinalIgnoreCase))
                    background = false;
                else if (backgroundFlag.Equals("ASYNC", StringComparison.OrdinalIgnoreCase))
                    background = true;
                else
                {
                    while (!RespWriteUtils.WriteError($"ERR Invalid CLUSTER REPLICATE FLAG ({backgroundFlag}) not valid", ref dcurr, dend))
                        SendAndReset();
                    readHead = (int)(ptr - recvBufferPtr);
                    return true;
                }
            }
            readHead = (int)(ptr - recvBufferPtr);

            if (!clusterProvider.serverOptions.EnableAOF)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!clusterProvider.replicationManager.TryBeginReplicate(this, nodeid, background, false, out ReadOnlySpan<byte> errorMessage))
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
        }
        else if (param.SequenceEqual(CmdStrings.aofsync))
        {
            byte* ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out string nodeid, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadLongWithLengthHeader(out long nextAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (clusterProvider.serverOptions.EnableAOF)
            {
                clusterProvider.replicationManager.TryAddReplicationTask(nodeid, nextAddress, out AofSyncTaskInfo aofSyncTaskInfo);
                if (!clusterProvider.replicationManager.TryConnectToReplica(nodeid, nextAddress, aofSyncTaskInfo, out ReadOnlySpan<byte> errorMessage))
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.appendlog))
        {
            byte* ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out string nodeId, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadLongWithLengthHeader(out long previousAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadLongWithLengthHeader(out long currentAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadLongWithLengthHeader(out long nextAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte* record = null;
            int recordLength = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref record, ref recordLength, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            ClusterConfig currentConfig = clusterProvider.clusterManager.CurrentConfig;
            NodeRole localRole = currentConfig.LocalNodeRole;
            string primaryId = currentConfig.LocalNodePrimaryId;
            if (localRole != NodeRole.REPLICA)
            {
                // TODO: handle this
                //while (!RespWriteUtils.WriteError("ERR aofsync node not a replica"u8, ref dcurr, dend))
                //    SendAndReset();
            }
            else if (!primaryId.Equals(nodeId))
            {
                // TODO: handle this
                //while (!RespWriteUtils.WriteError($"ERR aofsync node replicating {primaryId}", ref dcurr, dend))
                //    SendAndReset();
            }
            else
            {
                clusterProvider.replicationManager.ProcessPrimaryStream(record, recordLength, previousAddress, currentAddress, nextAddress);
                //while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                //    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.initiate_replica_sync))
        {
            byte* ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out string nodeId, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadStringWithLengthHeader(out string primary_replid, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] cEntryByteArray, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadLongWithLengthHeader(out long replicaAofBeginAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadLongWithLengthHeader(out long replicaAofTailAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            var remoteEntry = CheckpointEntry.FromByteArray(cEntryByteArray);

            if (!clusterProvider.replicationManager.TryBeginReplicaSyncSession(
                nodeId, primary_replid, remoteEntry, replicaAofBeginAddress, replicaAofTailAddress, out ReadOnlySpan<byte> errorMessage))
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else if (param.SequenceEqual(CmdStrings.send_ckpt_metadata))
        {
            byte* ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] fileTokenBytes, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadIntWithLengthHeader(out int fileTypeInt, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] checkpointMetadata, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            var fileToken = new Guid(fileTokenBytes);
            var fileType = (CheckpointFileType)fileTypeInt;
            clusterProvider.replicationManager.ProcessCheckpointMetadata(fileToken, fileType, checkpointMetadata);
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
        }
        else if (param.SequenceEqual(CmdStrings.send_ckpt_file_segment))
        {
            byte* ptr = recvBufferPtr + readHead;
            Span<byte> data = default;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] fileTokenBytes, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadIntWithLengthHeader(out int ckptFileTypeInt, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadLongWithLengthHeader(out long startAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadSpanByteWithLengthHeader(ref data, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadIntWithLengthHeader(out int segmentId, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);
            var fileToken = new Guid(fileTokenBytes);
            var ckptFileType = (CheckpointFileType)ckptFileTypeInt;

            // Commenting due to high verbosity
            // logger?.LogTrace("send_ckpt_file_segment {fileToken} {ckptFileType} {startAddress} {dataLength}", fileToken, ckptFileType, startAddress, data.Length);
            clusterProvider.replicationManager.recvCheckpointHandler.ProcessFileSegments(segmentId, fileToken, ckptFileType, startAddress, data);
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
        }
        else if (param.SequenceEqual(CmdStrings.begin_replica_recover))
        {
            byte* ptr = recvBufferPtr + readHead;

            if (!RespReadUtils.ReadBoolWithLengthHeader(out bool recoverMainStoreFromToken, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadBoolWithLengthHeader(out bool recoverObjectStoreFromToken, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadBoolWithLengthHeader(out bool replayAOF, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadStringWithLengthHeader(out string primary_replid, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] cEntryByteArray, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadLongWithLengthHeader(out long beginAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadLongWithLengthHeader(out long tailAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            var entry = CheckpointEntry.FromByteArray(cEntryByteArray);
            long replicationOffset = clusterProvider.replicationManager.BeginReplicaRecover(
                recoverMainStoreFromToken,
                recoverObjectStoreFromToken,
                replayAOF,
                primary_replid,
                entry,
                beginAddress,
                tailAddress);
            while (!RespWriteUtils.WriteInteger(replicationOffset, ref dcurr, dend))
                SendAndReset();
        }
        else
            return false;

        return true;
    }
}