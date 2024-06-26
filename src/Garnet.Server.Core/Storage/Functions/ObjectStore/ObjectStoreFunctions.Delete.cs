﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// Object store functions
/// </summary>
public readonly unsafe partial struct ObjectStoreFunctions : IFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
{
    /// <inheritdoc />
    public bool SingleDeleter(ref byte[] key, ref IGarnetObject value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        => true;

    /// <inheritdoc />
    public void PostSingleDeleter(ref byte[] key, ref DeleteInfo deleteInfo)
    {
        if (!deleteInfo.RecordInfo.Modified)
            _functionsState.WatchVersionMap.IncrementVersion(deleteInfo.KeyHash);
        if (_functionsState.AppendOnlyFile != null)
            WriteLogDelete(ref key, deleteInfo.Version, deleteInfo.SessionID);
    }

    /// <inheritdoc />
    public bool ConcurrentDeleter(ref byte[] key, ref IGarnetObject value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
    {
        if (!deleteInfo.RecordInfo.Modified)
            _functionsState.WatchVersionMap.IncrementVersion(deleteInfo.KeyHash);
        if (_functionsState.AppendOnlyFile != null)
            WriteLogDelete(ref key, deleteInfo.Version, deleteInfo.SessionID);
        _functionsState.ObjectStoreSizeTracker?.AddTrackedSize(-value.Size);
        return true;
    }
}