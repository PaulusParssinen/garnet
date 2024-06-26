﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.ComponentModel;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Tsavorite;

namespace Garnet.Cluster;

internal sealed unsafe class ReceiveCheckpointHandler
{
    private readonly ClusterProvider clusterProvider;
    private IDevice writeIntoCkptDevice = null;
    private SemaphoreSlim writeCheckpointSemaphore = null;
    private SectorAlignedBufferPool writeCheckpointBufferPool = null;
    private readonly ILogger logger;

    public ReceiveCheckpointHandler(ClusterProvider clusterProvider, ILogger logger = null)
    {
        this.clusterProvider = clusterProvider;
        this.logger = logger;
    }

    public void Dispose()
    {
        writeCheckpointSemaphore?.Dispose();
        writeCheckpointBufferPool?.Free();
        writeCheckpointBufferPool = null;
        CloseDevice();
    }

    public void CloseDevice()
    {
        writeIntoCkptDevice?.Dispose();
        writeIntoCkptDevice = null;
    }

    /// <summary>
    /// Process file segments send from primary
    /// </summary>
    public void ProcessFileSegments(int segmentId, Guid token, CheckpointFileType type, long startAddress, Span<byte> data)
    {
        clusterProvider.replicationManager.UpdateLastPrimarySyncTime();
        if (writeIntoCkptDevice == null)
        {
            Debug.Assert(writeIntoCkptDevice == null);
            writeIntoCkptDevice = clusterProvider.replicationManager.GetInitializedSegmentFileDevice(token, type);
        }

        if (data.Length == 0)
        {
            Debug.Assert(writeIntoCkptDevice != null);
            CloseDevice();
            return;
        }

        Debug.Assert(writeIntoCkptDevice != null);
        WriteInto(writeIntoCkptDevice, (ulong)startAddress, data, data.Length, segmentId);
    }

    /// <summary>
    /// Note: pads the bytes with zeros to achieve sector alignment
    /// </summary>
    private unsafe void WriteInto(IDevice device, ulong address, Span<byte> buffer, int size, int segmentId = -1)
    {
        if (writeCheckpointBufferPool == null)
            writeCheckpointBufferPool = new SectorAlignedBufferPool(1, (int)device.SectorSize);

        long numBytesToWrite = size;
        numBytesToWrite = (numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1);

        SectorAlignedMemory pbuffer = writeCheckpointBufferPool.Get((int)numBytesToWrite);
        fixed (byte* bufferRaw = buffer)
        {
            Buffer.MemoryCopy(bufferRaw, pbuffer.AlignedPointer, size, size);
        }

        if (writeCheckpointSemaphore == null) writeCheckpointSemaphore = new(0);

        if (segmentId == -1)
            device.WriteAsync((IntPtr)pbuffer.AlignedPointer, address, (uint)numBytesToWrite, IOCallback, null);
        else
            device.WriteAsync((IntPtr)pbuffer.AlignedPointer, segmentId, address, (uint)numBytesToWrite, IOCallback, null);
        writeCheckpointSemaphore.Wait();

        pbuffer.Return();
    }

    private unsafe void IOCallback(uint errorCode, uint numBytes, object context)
    {
        if (errorCode != 0)
        {
            string errorMessage = new Win32Exception((int)errorCode).Message;
            logger?.LogError("[Replica] OverlappedStream GetQueuedCompletionStatus error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
        }
        writeCheckpointSemaphore.Release();
    }
}