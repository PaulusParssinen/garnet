// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite;

/// <summary>
///  Represents a entry that can be serialized directly onto TsavoriteLog when enqueuing
/// </summary>
public interface ILogEnqueueEntry
{
    /// <returns> the size in bytes after serialization onto TsavoriteLog</returns>
    public int SerializedLength { get; }

    /// <summary>
    /// Serialize the entry onto TsavoriteLog.
    /// </summary>
    /// <param name="dest">Memory buffer of TsavoriteLog to serialize onto. Guaranteed to have at least SerializedLength() many bytes</param>
    public void SerializeTo(Span<byte> dest);
}