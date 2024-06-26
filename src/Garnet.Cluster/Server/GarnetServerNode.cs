﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Security;
using Garnet.Common;
using Microsoft.Extensions.Logging;

namespace Garnet.Cluster;

internal sealed class GarnetServerNode
{
    private readonly ClusterProvider clusterProvider;
    private readonly GarnetClient gc;
    private long gossip_send;
    private long gossip_recv;
    private CancellationTokenSource cts = new();
    private CancellationTokenSource internalCts = new();
    private volatile int initialized = 0;
    private readonly ILogger logger = null;
    private int disposeCount = 0;
    private ClusterConfig lastConfig = null;
    private SingleWriterMultiReaderLock meetLock;

    public bool IsConnected => gc.IsConnected;

    public long GossipSend => gossip_send;

    public long GossipRecv => gossip_recv;

    public GarnetClient Client => gc;

    /// <summary>
    /// Nodeid of remote node
    /// </summary>
    public string NodeId;

    /// <summary>
    /// Address of remote node
    /// </summary>
    public string address;

    /// <summary>
    /// Port of remote node
    /// </summary>
    public int port;

    public bool gossipSendSuccess = false;
    public Task gossipTask = null;

    public GarnetServerNode(ClusterProvider clusterProvider, string address, int port, SslClientAuthenticationOptions tlsOptions, ILogger logger = null)
    {
        this.clusterProvider = clusterProvider;
        this.address = address;
        this.port = port;
        gc = new GarnetClient(
            address, port, tlsOptions,
            sendPageSize: 1 << 17,
            maxOutstandingTasks: 8,
            authUsername: clusterProvider.clusterManager.clusterProvider.ClusterUsername,
            authPassword: clusterProvider.clusterManager.clusterProvider.ClusterPassword,
            logger: logger);
        initialized = 0;
        this.logger = logger;
        ResetCts();
    }

    public void Dispose()
    {
        if (Interlocked.Increment(ref disposeCount) != 1)
            logger?.LogTrace("GarnetServerNode.Dispose called multiple times");
        try
        {
            cts?.Cancel();
            cts?.Dispose();
            internalCts?.Cancel();
            internalCts?.Dispose();
            gc?.Dispose();
        }
        catch { }
    }

    public void Initialize()
    {
        //Ensure initialize executes only once
        if (Interlocked.CompareExchange(ref initialized, 1, 0) != 0) return;

        cts = CancellationTokenSource.CreateLinkedTokenSource(clusterProvider.clusterManager.ctsGossip.Token, internalCts.Token);
        gc.ReconnectAsync().WaitAsync(clusterProvider.clusterManager.gossipDelay, cts.Token).GetAwaiter().GetResult();
    }

    public void UpdateGossipSend() => gossip_send = DateTimeOffset.UtcNow.Ticks;
    public void UpdateGossipRecv() => gossip_recv = DateTimeOffset.UtcNow.Ticks;

    public void ResetCts()
    {
        bool internalCtsDisposed = false;
        internalCts.Cancel();
        if (!internalCts.TryReset())
        {
            internalCts.Dispose();
            internalCts = new();
            internalCtsDisposed = true;
        }

        if (internalCtsDisposed || !cts.TryReset())
        {
            cts.Cancel();
            cts.Dispose();
            cts = CancellationTokenSource.CreateLinkedTokenSource(clusterProvider.clusterManager.ctsGossip.Token, internalCts.Token);
        }
        gossipTask = null;
    }

    public MemoryResult<byte> TryMeet(byte[] configByteArray)
    {
        try
        {
            _ = meetLock.TryWriteLock();
            UpdateGossipSend();
            MemoryResult<byte> resp = gc.GossipWithMeet(configByteArray).WaitAsync(clusterProvider.clusterManager.gossipDelay, cts.Token).GetAwaiter().GetResult();
            return resp;
        }
        finally
        {
            meetLock.WriteUnlock();
        }
    }

    /// <summary>
    /// Keep track of updated config per connection. Useful when gossip sampling so as to ensure updates are propagated
    /// </summary>
    private byte[] GetMostRecentConfig()
    {
        ClusterConfig conf = clusterProvider.clusterManager.CurrentConfig;
        byte[] byteArray;
        if (conf != lastConfig)
        {
            if (clusterProvider.replicationManager != null) conf.LazyUpdateLocalReplicationOffset(clusterProvider.replicationManager.ReplicationOffset);
            byteArray = conf.ToByteArray();
            lastConfig = conf;
        }
        else
        {
            byteArray = Array.Empty<byte>();
        }
        return byteArray;
    }

    /// <summary>
    /// Send gossip message or process response and send again.
    /// </summary>
    public bool TryGossip()
    {
        byte[] configByteArray = GetMostRecentConfig();
        Task task = gossipTask;
        // If first time we are sending gossip make sure to send latest version
        if (task == null)
        {
            // Issue first time gossip
            byte[] configArray = clusterProvider.clusterManager.CurrentConfig.ToByteArray();
            gossipTask = Gossip(configArray);
            UpdateGossipSend();
            clusterProvider.clusterManager.gossipStats.gossip_full_send++;
            // Track bytes send
            clusterProvider.clusterManager.gossipStats.UpdateGossipBytesSend(configArray.Length);
            return true;
        }
        else if (task.Status == TaskStatus.RanToCompletion)
        {
            UpdateGossipRecv();

            // Issue new gossip that can be either zero packet size or an updated configuration
            gossipTask = Gossip(configByteArray);
            UpdateGossipSend();

            // Track number of full vs empty (ping) sends
            if (configByteArray.Length > 0)
                clusterProvider.clusterManager.gossipStats.gossip_full_send++;
            else
                clusterProvider.clusterManager.gossipStats.gossip_empty_send++;

            // Track bytes send
            clusterProvider.clusterManager.gossipStats.UpdateGossipBytesSend(configByteArray.Length);
            return true;
        }
        logger?.LogWarning(task.Exception, "GOSSIP round faulted");
        ResetCts();
        gossipTask = null;
        return false;
    }

    private Task Gossip(byte[] configByteArray)
    {
        return gc.Gossip(configByteArray).ContinueWith(t =>
        {
            try
            {
                MemoryResult<byte> resp = t.Result;
                if (resp.Length > 0)
                {
                    clusterProvider.clusterManager.gossipStats.UpdateGossipBytesRecv(resp.Length);
                    byte[] returnedConfigArray = resp.Span.ToArray();
                    var other = ClusterConfig.FromByteArray(returnedConfigArray);
                    ClusterConfig current = clusterProvider.clusterManager.CurrentConfig;
                    // Check if gossip is from a node that is known and trusted before merging
                    if (current.IsKnown(other.LocalNodeId))
                        clusterProvider.clusterManager.TryMerge(ClusterConfig.FromByteArray(returnedConfigArray));
                    else
                        logger?.LogWarning("Received gossip from unknown node: {node-id}", other.LocalNodeId);
                }
                resp.Dispose();
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "GOSSIP faulted processing response");
            }
        }, TaskContinuationOptions.OnlyOnRanToCompletion).WaitAsync(clusterProvider.clusterManager.gossipDelay, cts.Token);
    }
}