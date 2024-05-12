// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.Device;

/// <summary>
/// 
/// </summary>
public sealed class NullDevice : StorageDeviceBase
{
    /// <summary>
    /// 
    /// </summary>
    public NullDevice() : base("null", 512, Devices.CAPACITY_UNSPECIFIED)
    {
    }

    /// <summary>
    /// 
    /// </summary>
    public override unsafe void ReadAsync(int segmentId, ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, DeviceIOCompletionCallback callback, object context)
    {
        callback(0, aligned_read_length, context);
    }

    /// <summary>
    /// 
    /// </summary>
    public override unsafe void WriteAsync(IntPtr alignedSourceAddress, int segmentId, ulong alignedDestinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
    {
        callback(0, numBytesToWrite, context);
    }

    /// <summary>
    /// <see cref="IDevice.RemoveSegment(int)"/>
    /// </summary>
    public override void RemoveSegment(int segment)
    {
        // No-op
    }

    /// <summary>
    /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
    /// </summary>
    public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result) => callback(result);

    /// <inheritdoc />
    public override void Dispose()
    {
    }
}