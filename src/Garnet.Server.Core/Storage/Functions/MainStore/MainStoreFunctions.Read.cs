// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// Callback functions for main store
/// </summary>
public readonly unsafe partial struct MainStoreFunctions : IFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
{
    /// <inheritdoc />
    public bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
    {
        if (value.MetadataSize != 0 && CheckExpiry(ref value))
            return false;

        RespCommand cmd = ((RespInputHeader*)input.ToPointer())->cmd;
        if ((byte)cmd >= CustomCommandManager.StartOffset)
        {
            int valueLength = value.LengthWithoutMetadata;
            (IMemoryOwner<byte> Memory, int Length) outp = (dst.Memory, 0);
            bool ret = _functionsState.CustomCommands[(byte)cmd - CustomCommandManager.StartOffset].functions.Reader(key.AsReadOnlySpan(), input.AsReadOnlySpan()[RespInputHeader.Size..], value.AsReadOnlySpan(), ref outp, ref readInfo);
            Debug.Assert(valueLength <= value.LengthWithoutMetadata);
            dst.Memory = outp.Memory;
            dst.Length = outp.Length;
            return ret;
        }

        if (input.Length == 0)
            CopyRespTo(ref value, ref dst);
        else
            CopyRespToWithInput(ref input, ref value, ref dst);

        return true;
    }

    /// <inheritdoc />
    public bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
    {
        if (value.MetadataSize != 0 && CheckExpiry(ref value))
        {
            // TODO: we can proactively expire if we wish, but if we do, we need to write WAL entry
            // readInfo.Action = ReadAction.Expire;
            return false;
        }

        RespCommand cmd = ((RespInputHeader*)input.ToPointer())->cmd;
        if ((byte)cmd >= CustomCommandManager.StartOffset)
        {
            int valueLength = value.LengthWithoutMetadata;
            (IMemoryOwner<byte> Memory, int Length) outp = (dst.Memory, 0);
            bool ret = _functionsState.CustomCommands[(byte)cmd - CustomCommandManager.StartOffset].functions.Reader(key.AsReadOnlySpan(), input.AsReadOnlySpan()[RespInputHeader.Size..], value.AsReadOnlySpan(), ref outp, ref readInfo);
            Debug.Assert(valueLength <= value.LengthWithoutMetadata);
            dst.Memory = outp.Memory;
            dst.Length = outp.Length;
            return ret;
        }

        if (input.Length == 0)
            CopyRespTo(ref value, ref dst);
        else
            CopyRespToWithInput(ref input, ref value, ref dst);

        return true;
    }
}