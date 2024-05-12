// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Text;
using Garnet.Server;
using Microsoft.Extensions.Logging;
using Tsavorite;

namespace Garnet.Cluster;

internal sealed partial class ReplicationManager : IDisposable
{
    readonly ClusterProvider clusterProvider;
    readonly StoreWrapper storeWrapper;
    readonly AofProcessor aofProcessor;
    readonly CheckpointStore checkpointStore;

    readonly CancellationTokenSource ctsRepManager = new();

    readonly ILogger logger;
    bool _disposed;

    private long primary_sync_last_time;

    internal long LastPrimarySyncSeconds => recovering ? (DateTime.UtcNow.Ticks - primary_sync_last_time) / TimeSpan.TicksPerSecond : 0;

    internal void UpdateLastPrimarySyncTime() => this.primary_sync_last_time = DateTime.UtcNow.Ticks;

    public bool recovering;
    private long replicationOffset;
    public long ReplicationOffset
    {
        get
        {
            // Primary tracks replicationOffset indirectly through AOF tailAddress
            // Replica will adjust replication offset as it receives data from primary (TODO: since AOFs are synced this might obsolete)
            NodeRole role = clusterProvider.clusterManager.CurrentConfig.LocalNodeRole;
            return role == NodeRole.PRIMARY ?
                (clusterProvider.serverOptions.EnableAOF && storeWrapper.appendOnlyFile.TailAddress > kFirstValidAofAddress ? storeWrapper.appendOnlyFile.TailAddress : kFirstValidAofAddress) :
                replicationOffset;
        }

        set { replicationOffset = value; }
    }

    /// <summary>
    /// Replication offset until which AOF address is valid for old primary if failover has occurred
    /// </summary>
    public long ReplicationOffset2
    {
        get { return currentReplicationConfig.replicationOffset2; }
    }

    public string PrimaryReplId => currentReplicationConfig.primary_replid;
    public string PrimaryReplId2 => currentReplicationConfig.primary_replid2;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReplicationLogCheckpointManager GetCkptManager(StoreType storeType)
    {
        return storeType switch
        {
            StoreType.Main => (ReplicationLogCheckpointManager)storeWrapper.store.CheckpointManager,
            StoreType.Object => (ReplicationLogCheckpointManager)storeWrapper.objectStore?.CheckpointManager,
            _ => throw new Exception($"GetCkptManager: unexpected state {storeType}")
        };
    }

    public long GetRecoveredSafeAofAddress()
    {
        long storeAofAddress = clusterProvider.replicationManager.GetCkptManager(StoreType.Main).RecoveredSafeAofAddress;
        long objectStoreAofAddress = clusterProvider.serverOptions.DisableObjects ? clusterProvider.replicationManager.GetCkptManager(StoreType.Main).RecoveredSafeAofAddress : long.MaxValue;
        return Math.Min(storeAofAddress, objectStoreAofAddress);
    }

    public long GetCurrentSafeAofAddress()
    {
        long storeAofAddress = clusterProvider.replicationManager.GetCkptManager(StoreType.Main).CurrentSafeAofAddress;
        long objectStoreAofAddress = clusterProvider.serverOptions.DisableObjects ? clusterProvider.replicationManager.GetCkptManager(StoreType.Main).CurrentSafeAofAddress : long.MaxValue;
        return Math.Min(storeAofAddress, objectStoreAofAddress);
    }

    public ReplicationManager(ClusterProvider clusterProvider, ILogger logger = null)
    {
        GarnetServerOptions opts = clusterProvider.serverOptions;
        this.clusterProvider = clusterProvider;
        this.storeWrapper = clusterProvider.storeWrapper;
        aofProcessor = new AofProcessor(storeWrapper, recordToAof: false, logger: logger);
        replicaSyncSessionTaskStore = new ReplicaSyncSessionTaskStore(storeWrapper, clusterProvider, logger);

        ReplicationOffset = 0;

        // Set the appendOnlyFile field for all stores
        clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main).checkpointVersionShift = CheckpointVersionShift;
        if (storeWrapper.objectStore != null)
            clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).checkpointVersionShift = CheckpointVersionShift;

        // If starts as replica, it cannot serve until it is connected to primary
        if (clusterProvider.clusterManager.CurrentConfig.LocalNodeRole == NodeRole.REPLICA)
            recovering = true;

        this.logger = logger;

        checkpointStore = new CheckpointStore(storeWrapper, clusterProvider, true, logger);
        aofTaskStore = new(clusterProvider, 1, logger);

        string clusterFolder = "/cluster";
        string clusterDataPath = opts.CheckpointDir + clusterFolder;
        INamedDeviceFactory deviceFactory = opts.GetInitializedDeviceFactory(clusterDataPath);
        replicationConfigDevice = deviceFactory.Get(new FileDescriptor(directoryName: "", fileName: "replication.conf"));
        pool = new(1, (int)replicationConfigDevice.SectorSize);

        bool recoverConfig = replicationConfigDevice.GetFileSize(0) > 0;
        if (!recoverConfig)
        {
            InitializeReplicationHistory();
        }
        else
        {
            RecoverReplicationHistory();
        }

        // After initializing replication history propagate replicationId to ReplicationLogCheckpointManager
        SetPrimaryReplicationId();
    }

    void CheckpointVersionShift(bool isMainStore, long oldVersion, long newVersion)
    {
        if (clusterProvider.clusterManager.CurrentConfig.LocalNodeRole == NodeRole.REPLICA)
            return;
        storeWrapper.EnqueueCommit(isMainStore, newVersion);
    }

    public void Reset()
    {
        recovering = false;
    }

    public void Dispose()
    {
        _disposed = true;

        replicationConfigDevice.Dispose();
        pool.Free();

        checkpointStore.WaitForReplicas();
        DisposeReplicaSyncSessionTasks();
        DisposeConnections();
        replicaSyncSessionTaskStore.Dispose();
        ctsRepManager.Dispose();
        aofProcessor?.Dispose();
    }

    public void DisposeConnections()
    {
        ctsRepManager.Cancel();
        aofTaskStore.Dispose();
    }

    /// <summary>
    /// Main recover method for replication
    /// </summary>
    public void Recover()
    {
        NodeRole nodeRole = clusterProvider.clusterManager.CurrentConfig.LocalNodeRole;

        switch (nodeRole)
        {
            case NodeRole.PRIMARY:
                PrimaryRecover();
                break;
            case NodeRole.REPLICA:
                // We will instead recover as part of TryConnectToPrimary instead
                // ReplicaRecover();
                break;
            default:
                logger?.LogError($"Not valid role for node {nodeRole}");
                throw new Exception($"Not valid role for node {nodeRole}");
        }
    }

    /// <summary>
    /// Primary recover
    /// </summary>
    private void PrimaryRecover()
    {
        storeWrapper.RecoverCheckpoint();
        storeWrapper.RecoverAOF();
        if (clusterProvider.serverOptions.EnableAOF)
        {
            // If recovered checkpoint corresponds to an unavailable AOF address, we initialize AOF to that address
            long recoveredSafeAofAddress = GetRecoveredSafeAofAddress();
            if (storeWrapper.appendOnlyFile.TailAddress < recoveredSafeAofAddress)
                storeWrapper.appendOnlyFile.Initialize(recoveredSafeAofAddress, recoveredSafeAofAddress);
            logger?.LogInformation("Recovered AOF: begin address = {beginAddress}, tail address = {tailAddress}", storeWrapper.appendOnlyFile.BeginAddress, storeWrapper.appendOnlyFile.TailAddress);
            ReplicationOffset = storeWrapper.ReplayAOF();
        }

        // First recover and then load latest checkpoint info in-memory
        InitializeCheckpointStore();
    }

    /// <summary>
    /// Wait for local replication offset to sync with input value
    /// </summary>
    public async Task<long> WaitForReplicationOffset(long primaryReplicationOffset)
    {
        while (ReplicationOffset < primaryReplicationOffset)
        {
            if (ctsRepManager.IsCancellationRequested) return -1;
            await Task.Yield();
        }
        return ReplicationOffset;
    }

    /// <summary>
    /// Initiate connection with PRIMARY after restart
    /// </summary>
    public void Start()
    {
        if (clusterProvider.clusterManager == null)
            return;

        ClusterConfig current = clusterProvider.clusterManager.CurrentConfig;

        NodeRole localNodeRole = current.LocalNodeRole;
        string replicaOfNodeId = current.LocalNodePrimaryId;
        if (localNodeRole == NodeRole.REPLICA && replicaOfNodeId != null)
        {
            clusterProvider.replicationManager.recovering = true;
            clusterProvider.WaitForConfigTransition();
            if (!TryReplicateFromPrimary(out ReadOnlySpan<byte> errorMessage))
                logger?.LogError($"An error occurred at {nameof(ReplicationManager)}.{nameof(Start)} {{error}}", Encoding.ASCII.GetString(errorMessage));
        }
        else if (localNodeRole == NodeRole.PRIMARY && replicaOfNodeId == null)
        {
            List<string> replicaIds = current.GetLocalNodeReplicaIds();
            foreach (string replicaId in replicaIds)
            {
                // TODO: Initiate AOF sync task correctly when restarting primary
                if (clusterProvider.replicationManager.TryAddReplicationTask(replicaId, 0, out AofSyncTaskInfo aofSyncTaskInfo))
                {
                    if (!TryConnectToReplica(replicaId, 0, aofSyncTaskInfo, out ReadOnlySpan<byte> errorMessage))
                        logger?.LogError(Encoding.ASCII.GetString(errorMessage));
                }
            }
        }
        else
        {
            logger?.LogWarning("Replication manager starting configuration inconsistent role:{role} replicaOfId:{replicaOfNodeId}", replicaOfNodeId, localNodeRole);
        }
    }
}