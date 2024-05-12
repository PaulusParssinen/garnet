// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite;

[StructLayout(LayoutKind.Explicit)]
internal struct AtomicOwner
{
    [FieldOffset(0)]
    private int owner;
    [FieldOffset(4)]
    private int count;
    [FieldOffset(0)]
    private long atomic;

    /// <summary>
    /// Enqueue token
    /// true: success + caller is new owner
    /// false: success + someone else is owner
    /// </summary>
    public bool Enqueue()
    {
        while (true)
        {
            AtomicOwner older = this;
            AtomicOwner newer = older;
            newer.count++;
            if (older.owner == 0)
                newer.owner = 1;

            if (Interlocked.CompareExchange(ref atomic, newer.atomic, older.atomic) == older.atomic)
            {
                return older.owner == 0;
            }
        }
    }

    /// <summary>
    /// Dequeue token (caller is/remains owner)
    /// true: successful dequeue
    /// false: failed dequeue
    /// </summary>
    public bool Dequeue()
    {
        while (true)
        {
            AtomicOwner older = this;
            AtomicOwner newer = older;
            newer.count--;

            if (Interlocked.CompareExchange(ref atomic, newer.atomic, older.atomic) == older.atomic)
            {
                return newer.count > 0;
            }
        }
    }

    /// <summary>
    /// Release queue ownership
    /// true: successful release
    /// false: failed release
    /// </summary>
    public bool Release()
    {
        while (true)
        {
            AtomicOwner older = this;
            AtomicOwner newer = older;

            if (newer.count > 0)
                return false;

            if (newer.owner == 0)
                throw new TsavoriteException("Invalid release by non-owner thread");
            newer.owner = 0;

            if (Interlocked.CompareExchange(ref atomic, newer.atomic, older.atomic) == older.atomic)
            {
                return true;
            }
        }
    }
}