﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Extensions.Logging;
using Tsavorite;

namespace Garnet.Cluster;

internal static class ClusterUtils
{
    public static byte[] ReadDevice(IDevice device, SectorAlignedBufferPool pool, ILogger logger = null)
    {
        ReadInto(device, pool, 0, out byte[] writePad, sizeof(int), logger);
        int size = BitConverter.ToInt32(writePad, 0);
        byte[] body;
        if (writePad.Length >= size + sizeof(int))
            body = writePad;
        else
            ReadInto(device, pool, 0, out body, size + sizeof(int), logger);
        return new Span<byte>(body)[sizeof(int)..].ToArray();
    }

    /// <summary>
    /// Note: pads the bytes with zeros to achieve sector alignment
    /// </summary>
    public static unsafe void WriteInto(IDevice device, SectorAlignedBufferPool pool, ulong address, byte[] buffer, int size = 0, ILogger logger = null)
    {
        if (size == 0) size = buffer.Length;
        byte[] _buffer = new byte[size + sizeof(int)];
        byte[] len = BitConverter.GetBytes(size);
        Array.Copy(len, _buffer, len.Length);
        Array.Copy(buffer, 0, _buffer, len.Length, size);
        size += len.Length;

        long numBytesToWrite = size;
        numBytesToWrite = (numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1);

        SectorAlignedMemory pbuffer = pool.Get((int)numBytesToWrite);
        fixed (byte* bufferRaw = _buffer)
        {
            Buffer.MemoryCopy(bufferRaw, pbuffer.AlignedPointer, size, size);
        }
        using var semaphore = new SemaphoreSlim(0);
        device.WriteAsync((IntPtr)pbuffer.AlignedPointer, address, (uint)numBytesToWrite, logger == null ? IOCallback : logger.IOCallback, semaphore);
        semaphore.Wait();

        pbuffer.Return();
    }


    /// <summary>
    /// Note: will read potentially more data (based on sector alignment)
    /// </summary>
    private static unsafe void ReadInto(IDevice device, SectorAlignedBufferPool pool, ulong address, out byte[] buffer, int size, ILogger logger = null)
    {
        using var semaphore = new SemaphoreSlim(0);
        long numBytesToRead = size;
        numBytesToRead = (numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1);

        SectorAlignedMemory pbuffer = pool.Get((int)numBytesToRead);
        device.ReadAsync(address, (IntPtr)pbuffer.AlignedPointer,
            (uint)numBytesToRead, logger == null ? IOCallback : logger.IOCallback, semaphore);
        semaphore.Wait();

        buffer = new byte[numBytesToRead];
        fixed (byte* bufferRaw = buffer)
            Buffer.MemoryCopy(pbuffer.AlignedPointer, bufferRaw, numBytesToRead, numBytesToRead);
        pbuffer.Return();
    }

    private static void IOCallback(uint errorCode, uint numBytes, object context)
    {
        ((SemaphoreSlim)context).Release();
    }
}

internal static class LoggerExtensions
{
    public static void IOCallback(this ILogger logger, uint errorCode, uint numBytes, object context)
    {
        if (errorCode != 0)
        {
            string errorMessage = new Win32Exception((int)errorCode).Message;
            logger.LogError("OverlappedStream GetQueuedCompletionStatus error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
        }
        ((SemaphoreSlim)context).Release();
    }
}