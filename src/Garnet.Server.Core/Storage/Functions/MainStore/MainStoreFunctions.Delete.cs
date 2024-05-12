// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// Callback functions for main store
/// </summary>
public readonly unsafe partial struct MainStoreFunctions : IFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
{
    /// <inheritdoc />
    public bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
    {
        _functionsState.WatchVersionMap.IncrementVersion(deleteInfo.KeyHash);
        return true;
    }

    /// <inheritdoc />
    public void PostSingleDeleter(ref SpanByte key, ref DeleteInfo deleteInfo)
    {
        if (_functionsState.AppendOnlyFile != null)
            WriteLogDelete(ref key, deleteInfo.Version, deleteInfo.SessionID);
    }

    /// <inheritdoc />
    public bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
    {
        if (!deleteInfo.RecordInfo.Modified)
            _functionsState.WatchVersionMap.IncrementVersion(deleteInfo.KeyHash);
        if (_functionsState.AppendOnlyFile != null)
            WriteLogDelete(ref key, deleteInfo.Version, deleteInfo.SessionID);
        return true;
    }
}