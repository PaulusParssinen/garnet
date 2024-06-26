﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Reflection;
using Microsoft.Extensions.Logging;
using Tsavorite;
using Tsavorite.Device;

namespace Garnet.Common;

public enum FileLocationType
{
    Local,
    EmbeddedResource
}

/// <summary>
/// Interface for reading / writing into local / remote files
/// </summary>
public interface IStreamProvider
{
    /// <summary>
    /// Read data from file specified in path
    /// </summary>
    /// <param name="path">Path to file</param>
    /// <returns>Stream object</returns>
    Stream Read(string path);

    /// <summary>
    /// Write data into file specified in path
    /// </summary>
    /// <param name="path">Path to file</param>
    /// <param name="data">Data to write</param>
    void Write(string path, byte[] data);
}

/// <summary>
/// Base StreamProvider class containing common logic between stream providers 
/// </summary>
internal abstract class StreamProviderBase : IStreamProvider
{
    protected const int MaxConfigFileSizeAligned = 262144;

    public Stream Read(string path)
    {
        using IDevice device = GetDevice(path);
        var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
        ReadInto(device, pool, 0, out byte[] buffer, MaxConfigFileSizeAligned);
        pool.Free();

        // Remove trailing zeros
        int lastIndex = Array.FindLastIndex(buffer, b => b != 0);
        var stream = new MemoryStream(buffer, 0, lastIndex + 1);
        return stream;
    }

    public unsafe void Write(string path, byte[] data)
    {
        using IDevice device = GetDevice(path);
        long bytesToWrite = GetBytesToWrite(data, device);
        var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);

        // Get a sector-aligned buffer from the pool and copy _buffer into it.
        SectorAlignedMemory buffer = pool.Get((int)bytesToWrite);
        fixed (byte* bufferRaw = data)
        {
            Buffer.MemoryCopy(bufferRaw, buffer.AlignedPointer, data.Length, data.Length);
        }

        // Write to the device and wait for the device to signal the semaphore that the write is complete.
        using var semaphore = new SemaphoreSlim(0);
        device.WriteAsync((IntPtr)buffer.AlignedPointer, 0, (uint)bytesToWrite, IOCallback, semaphore);
        semaphore.Wait();

        // Free the sector-aligned buffer
        buffer.Return();
        pool.Free();
    }

    protected abstract IDevice GetDevice(string path);

    protected abstract long GetBytesToWrite(byte[] bytes, IDevice device);

    protected static unsafe void ReadInto(IDevice device, SectorAlignedBufferPool pool, ulong address, out byte[] buffer, int size, ILogger logger = null)
    {
        using var semaphore = new SemaphoreSlim(0);
        long numBytesToRead = size;
        numBytesToRead = (numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1);

        SectorAlignedMemory pbuffer = pool.Get((int)numBytesToRead);
        device.ReadAsync(address, (IntPtr)pbuffer.AlignedPointer,
            (uint)numBytesToRead, IOCallback, semaphore);
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

/// <summary>
/// Provides a StreamProvider instance
/// </summary>
public class StreamProviderFactory
{
    /// <summary>
    /// Get a StreamProvider instance
    /// </summary>
    /// <param name="locationType">Type of location of files the stream provider reads from / writes to</param>
    /// <param name="resourceAssembly">Assembly from which to load the embedded resource, if applicable</param>
    /// <returns>StreamProvider instance</returns>
    public static IStreamProvider GetStreamProvider(FileLocationType locationType, Assembly resourceAssembly = null)
    {
        switch (locationType)
        {
            case FileLocationType.Local:
                return new LocalFileStreamProvider();
            case FileLocationType.EmbeddedResource:
                return new EmbeddedResourceStreamProvider(resourceAssembly);
            default:
                throw new NotImplementedException();
        }
    }
}

/// <summary>
/// StreamProvider for reading / writing files locally
/// </summary>
internal class LocalFileStreamProvider : StreamProviderBase
{
    protected override IDevice GetDevice(string path)
    {
        var fileInfo = new FileInfo(path);

        INamedDeviceFactory settingsDeviceFactoryCreator = new LocalStorageNamedDeviceFactory(disableFileBuffering: false);
        settingsDeviceFactoryCreator.Initialize("");
        var settingsDevice = settingsDeviceFactoryCreator.Get(new FileDescriptor(fileInfo.DirectoryName, fileInfo.Name));
        settingsDevice.Initialize(-1, epoch: null, omitSegmentIdFromFilename: true);
        return settingsDevice;
    }

    protected override long GetBytesToWrite(byte[] bytes, IDevice device)
    {
        return bytes.Length;
    }
}

/// <summary>
/// StreamProvider for reading / writing files as embedded resources in executing assembly
/// </summary>
internal class EmbeddedResourceStreamProvider : IStreamProvider
{
    private readonly Assembly assembly;

    public EmbeddedResourceStreamProvider(Assembly assembly)
    {
        this.assembly = assembly;
    }

    public Stream Read(string path)
    {
        string resourceName = assembly.GetManifestResourceNames()
            .FirstOrDefault(rn => rn.EndsWith($".{path}"));
        if (resourceName == null) return null;

        return assembly.GetManifestResourceStream(resourceName);
    }

    public void Write(string path, byte[] data)
    {
        string resourceName = assembly.GetManifestResourceNames()
            .FirstOrDefault(rn => rn.EndsWith($".{path}"));
        if (resourceName == null) return;

        using Stream stream = assembly.GetManifestResourceStream(resourceName);
        if (stream != null)
            stream.Write(data, 0, data.Length);
    }
}