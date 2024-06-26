﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// A container per session to store information of watched keys
/// </summary>
internal sealed unsafe class WatchedKeysContainer
{
    /// <summary>
    /// Array to keep slice of keys inside keyBuffer
    /// </summary>
    private WatchedKeySlice[] keySlices;

    /// <summary>
    /// Array to keep slice of keys inside keyBuffer
    /// </summary>
    private readonly WatchVersionMap versionMap;
    private readonly int initialWatchBufferSize = 1 << 16;
    private readonly int initialSliceBufferSize;
    private int sliceBufferSize;
    private int watchBufferSize;
    private byte[] watchBuffer;
    private byte* watchBufferPtr;
    private int watchBufferHeadAddress;
    private int sliceCount;

    public WatchedKeysContainer(int size, WatchVersionMap versionMap)
    {
        this.versionMap = versionMap;
        watchBufferHeadAddress = 0;
        sliceCount = 0;
        initialSliceBufferSize = size;
    }

    /// <summary>
    /// Reset watched keys
    /// </summary>
    public void Reset()
    {
        sliceCount = 0;
        watchBufferPtr -= watchBufferHeadAddress;
        watchBufferHeadAddress = 0;
    }

    public bool RemoveWatch(ArgSlice key)
    {
        for (int i = 0; i < sliceCount; i++)
        {
            if (key.ReadOnlySpan.SequenceEqual(keySlices[i].slice.ReadOnlySpan))
            {
                keySlices[i].type = 0;
                return true;
            }
        }
        return false;
    }

    public void AddWatch(ArgSlice key, StoreType type)
    {
        if (sliceCount >= sliceBufferSize)
        {
            // Double the struct buffer
            sliceBufferSize = sliceBufferSize == 0 ? initialSliceBufferSize : sliceBufferSize * 2;
            WatchedKeySlice[] _oldBuffer = keySlices;
            keySlices = GC.AllocateUninitializedArray<WatchedKeySlice>(sliceBufferSize, true);
            if (_oldBuffer != null) Array.Copy(_oldBuffer, keySlices, _oldBuffer.Length);
        }
        if (watchBufferHeadAddress + key.Length > watchBufferSize)
        {
            // Double the watch buffer
            watchBufferSize = watchBufferSize == 0 ? initialWatchBufferSize : watchBufferSize * 2;
            byte[] _oldBuffer = watchBuffer;
            watchBuffer = GC.AllocateUninitializedArray<byte>(watchBufferSize, true);
            byte* watchBufferPtrBase = (byte*)Unsafe.AsPointer(ref watchBuffer[0]);
            watchBufferPtr = watchBufferPtrBase + watchBufferHeadAddress;

            if (_oldBuffer != null)
            {
                Array.Copy(_oldBuffer, watchBuffer, _oldBuffer.Length);
                byte* oldWatchBufferPtrBase = (byte*)Unsafe.AsPointer(ref _oldBuffer[0]);

                // Update pointer for existing watches
                for (int i = 0; i < sliceCount; i++)
                    keySlices[i].slice.ptr = watchBufferPtrBase + (keySlices[i].slice.ptr - oldWatchBufferPtrBase);
            }
        }

        var slice = new ArgSlice(watchBufferPtr, key.Length);
        key.ReadOnlySpan.CopyTo(slice.Span);

        keySlices[sliceCount].slice = slice;
        keySlices[sliceCount].type = type;
        keySlices[sliceCount].hash = Utility.HashBytes(slice.ptr, slice.Length);
        keySlices[sliceCount].version = versionMap.ReadVersion(keySlices[sliceCount].hash);

        watchBufferPtr += key.Length;
        watchBufferHeadAddress += key.Length;
        sliceCount++;
    }

    /// <summary>
    /// Validate record version to validate that records are unmodified
    /// </summary>
    public bool ValidateWatchVersion()
    {
        for (int i = 0; i < sliceCount; i++)
        {
            WatchedKeySlice key = keySlices[i];
            if (key.type == 0) continue;
            if (versionMap.ReadVersion(key.hash) != key.version)
                return false;
        }
        return true;
    }

    public bool SaveKeysToLock(TransactionManager txnManager)
    {
        for (int i = 0; i < sliceCount; i++)
        {
            WatchedKeySlice watchedKeySlice = keySlices[i];
            if (watchedKeySlice.type == 0) continue;

            ArgSlice slice = keySlices[i].slice;
            if (watchedKeySlice.type == StoreType.Main || watchedKeySlice.type == StoreType.All)
                txnManager.SaveKeyEntryToLock(slice, false, LockType.Shared);
            if (watchedKeySlice.type == StoreType.Object || watchedKeySlice.type == StoreType.All)
                txnManager.SaveKeyEntryToLock(slice, true, LockType.Shared);
        }
        return true;
    }

    public bool SaveKeysToKeyList(TransactionManager txnManager)
    {
        for (int i = 0; i < sliceCount; i++)
        {
            txnManager.SaveKeyArgSlice(keySlices[i].slice);
        }
        return true;
    }
}