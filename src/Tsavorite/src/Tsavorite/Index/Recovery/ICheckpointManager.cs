// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite;

/// <summary>
/// Interface for users to control creation and retrieval of checkpoint-related data
/// Tsavorite calls this interface during checkpoint/recovery in this sequence:
/// 
/// Checkpoint:
///   InitializeIndexCheckpoint (for index checkpoints) -> 
///   GetIndexDevice (for index checkpoints) ->
///   InitializeLogCheckpoint (for log checkpoints) ->
///   GetSnapshotLogDevice (for log checkpoints in snapshot mode) ->
///   GetSnapshotObjectLogDevice (for log checkpoints in snapshot mode with objects) ->
///   CommitLogCheckpoint (for log checkpoints) ->
///   CommitIndexCheckpoint (for index checkpoints) ->
/// 
/// Recovery:
///   GetIndexCommitMetadata ->
///   GetLogCommitMetadata ->
///   GetIndexDevice ->
///   GetSnapshotLogDevice (for recovery in snapshot mode) ->
///   GetSnapshotObjectLogDevice (for recovery in snapshot mode with objects)
/// 
/// Provided devices will be closed directly by Tsavorite when done.
/// </summary>
public interface ICheckpointManager : IDisposable
{
    /// <summary>
    /// Initialize index checkpoint
    /// </summary>
    void InitializeIndexCheckpoint(Guid indexToken);

    /// <summary>
    /// Initialize log checkpoint (snapshot and fold-over)
    /// </summary>
    void InitializeLogCheckpoint(Guid logToken);

    /// <summary>
    /// Commit index checkpoint
    /// </summary>
    void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata);

    /// <summary>
    /// Commit log checkpoint (snapshot and fold-over)
    /// </summary>
    void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata);

    /// <summary>
    /// Callback to indicate version shift during checkpoint
    /// </summary>
    void CheckpointVersionShift(long oldVersion, long newVersion);

    /// <summary>
    /// Commit log incremental checkpoint (incremental snapshot)
    /// </summary>
    void CommitLogIncrementalCheckpoint(Guid logToken, long version, byte[] commitMetadata, DeltaLog deltaLog);

    /// <summary>
    /// Retrieve commit metadata for specified index checkpoint
    /// </summary>
    /// <param name="indexToken">Token</param>
    /// <returns>Metadata, or null if invalid</returns>
    byte[] GetIndexCheckpointMetadata(Guid indexToken);

    /// <summary>
    /// Retrieve commit metadata for specified log checkpoint
    /// </summary>
    /// <param name="logToken">Token</param>
    /// <param name="deltaLog">Delta log</param>
    /// <param name="scanDelta"> whether or not to scan through the delta log to acquire latest entry. make sure the delta log points to the tail address immediately following the returned metadata.</param>
    /// <param name="recoverTo"> version upper bound to scan for in the delta log. Function will return the largest version metadata no greater than the given version.</param>
    /// <returns>Metadata, or null if invalid</returns>
    byte[] GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta = false, long recoverTo = -1);

    /// <summary>
    /// Get list of index checkpoint tokens, in order of usage preference
    /// </summary>
    public IEnumerable<Guid> GetIndexCheckpointTokens();

    /// <summary>
    /// Get list of log checkpoint tokens, in order of usage preference
    /// </summary>
    public IEnumerable<Guid> GetLogCheckpointTokens();

    /// <summary>
    /// Provide device to store index checkpoint (including overflow buckets)
    /// </summary>
    IDevice GetIndexDevice(Guid indexToken);

    /// <summary>
    /// Provide device to store snapshot of log (required only for snapshot checkpoints)
    /// </summary>
    IDevice GetSnapshotLogDevice(Guid token);

    /// <summary>
    /// Provide device to store snapshot of object log (required only for snapshot checkpoints)
    /// </summary>
    IDevice GetSnapshotObjectLogDevice(Guid token);

    /// <summary>
    /// Provide device to store incremental (delta) snapshot of log (required only for incremental snapshot checkpoints)
    /// </summary>
    IDevice GetDeltaLogDevice(Guid token);

    /// <summary>
    /// Cleanup all data (subfolder) related to the given guid by this manager
    /// </summary>
    public void Purge(Guid token);

    /// <summary>
    /// Cleanup all data (subfolder) related to checkpoints by this manager
    /// </summary>
    public void PurgeAll();

    /// <summary>
    /// Initiatize manager on recovery (e.g., deleting other checkpoints)
    /// </summary>
    public void OnRecovery(Guid indexToken, Guid logToken);
}