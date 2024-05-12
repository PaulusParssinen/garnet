// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// Object store functions
/// </summary>
public readonly unsafe partial struct ObjectStoreFunctions : IFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
{
    /// <inheritdoc />
    public bool SingleWriter(ref byte[] key, ref SpanByte input, ref IGarnetObject src, ref IGarnetObject dst, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
    {
        dst = src;
        return true;
    }

    /// <inheritdoc />
    public void PostSingleWriter(ref byte[] key, ref SpanByte input, ref IGarnetObject src, ref IGarnetObject dst, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
    {
        if (reason != WriteReason.CopyToTail)
            _functionsState.WatchVersionMap.IncrementVersion(upsertInfo.KeyHash);
        if (reason == WriteReason.Upsert && _functionsState.AppendOnlyFile != null)
            WriteLogUpsert(ref key, ref input, ref src, upsertInfo.Version, upsertInfo.SessionID);

        if (reason == WriteReason.CopyToReadCache)
            _functionsState.ObjectStoreSizeTracker?.AddReadCacheTrackedSize(MemoryUtils.CalculateKeyValueSize(key, src));
        else
            _functionsState.ObjectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateKeyValueSize(key, src));
    }

    /// <inheritdoc />
    public bool ConcurrentWriter(ref byte[] key, ref SpanByte input, ref IGarnetObject src, ref IGarnetObject dst, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
    {
        dst = src;
        if (!upsertInfo.RecordInfo.Modified)
            _functionsState.WatchVersionMap.IncrementVersion(upsertInfo.KeyHash);
        if (_functionsState.AppendOnlyFile != null)
            WriteLogUpsert(ref key, ref input, ref src, upsertInfo.Version, upsertInfo.SessionID);
        _functionsState.ObjectStoreSizeTracker?.AddTrackedSize(dst.Size - src.Size);
        return true;
    }
}