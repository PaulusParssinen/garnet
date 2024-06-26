﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Server;

/// <summary>
/// Store API
/// </summary>
public class StoreApi
{
    private readonly StoreWrapper storeWrapper;

    /// <summary>
    /// Construct new Store API instance
    /// </summary>
    public StoreApi(StoreWrapper storeWrapper)
    {
        this.storeWrapper = storeWrapper;
    }

    /// <summary>
    /// Commit AOF
    /// </summary>
    public void CommitAOF(bool spinWait = false) => storeWrapper.appendOnlyFile?.Commit(spinWait);

    /// <summary>
    /// Wait for commit
    /// </summary>
    public ValueTask WaitForCommitAsync(CancellationToken token = default) => storeWrapper.appendOnlyFile != null ? storeWrapper.appendOnlyFile.WaitForCommitAsync(token: token) : ValueTask.CompletedTask;

    /// <summary>
    /// Wait for commit
    /// </summary>
    public void WaitForCommit() => storeWrapper.appendOnlyFile?.WaitForCommit();

    /// <summary>
    /// Commit AOF
    /// </summary>
    public ValueTask CommitAOFAsync(CancellationToken token) => storeWrapper.appendOnlyFile != null ? storeWrapper.appendOnlyFile.CommitAsync(token) : ValueTask.CompletedTask;

    /// <summary>
    /// Flush DB (delete all keys)
    /// Optionally truncate log on disk. This is a destructive operation. Instead take a checkpoint if you are using checkpointing, as
    /// that will safely truncate the log on disk after the checkpoint.
    /// </summary>
    public void FlushDB(bool unsafeTruncateLog = false)
    {
        storeWrapper.store.Log.ShiftBeginAddress(storeWrapper.store.Log.TailAddress, truncateLog: unsafeTruncateLog);
        storeWrapper.objectStore?.Log.ShiftBeginAddress(storeWrapper.objectStore.Log.TailAddress, truncateLog: unsafeTruncateLog);
    }
}