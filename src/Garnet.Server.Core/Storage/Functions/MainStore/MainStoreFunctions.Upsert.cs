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
    public bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        => SpanByteFunctions<long>.DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo);

    /// <inheritdoc />
    public void PostSingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
    {
        _functionsState.WatchVersionMap.IncrementVersion(upsertInfo.KeyHash);
        if (reason == WriteReason.Upsert && _functionsState.AppendOnlyFile != null)
            WriteLogUpsert(ref key, ref input, ref src, upsertInfo.Version, upsertInfo.SessionID);
    }


    /// <inheritdoc />
    public bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
    {
        if (ConcurrentWriterWorker(ref src, ref dst, ref upsertInfo, ref recordInfo))
        {
            if (!upsertInfo.RecordInfo.Modified)
                _functionsState.WatchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (_functionsState.AppendOnlyFile != null)
                WriteLogUpsert(ref key, ref input, ref src, upsertInfo.Version, upsertInfo.SessionID);
            return true;
        }
        return false;
    }

    private static bool ConcurrentWriterWorker(ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        => SpanByteFunctions<long>.DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo);
}