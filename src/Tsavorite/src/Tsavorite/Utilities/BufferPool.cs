// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
#define CHECK_FREE      // disabled by default in Release due to overhead
#endif
// #define CHECK_FOR_LEAKS // disabled by default due to overhead

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite;

/// <summary>
/// Sector aligned memory allocator
/// </summary>
public sealed unsafe class SectorAlignedMemory
{
    // Byte #31 is used to denote free (1) or in-use (0) page
    private const int FreeBitMask = 1 << 31;

    public byte[] Buffer;
    internal GCHandle Handle;
    public int Offset;
    public byte* AlignedPointer;
    public int ValidOffset;
    public int RequiredBytes;
    public int AvailableBytes;
    
    private int level;
    internal int Level => level
#if CHECK_FREE
        & ~FreeBitMask
#endif
        ;

    internal SectorAlignedBufferPool pool;

#if CHECK_FREE
    internal bool Free
    {
        get => (level & FreeBitMask) != 0;
        set
        {
            if (value)
            {
                if (Free)
                    throw new TsavoriteException("Attempting to return an already-free block");
                level |= FreeBitMask;
            }
            else
            {
                if (!Free)
                    throw new TsavoriteException("Attempting to allocate an already-allocated block");
                level &= ~FreeBitMask;
            }
        }
    }
#endif // CHECK_FREE

    /// <summary>
    /// Default constructor
    /// </summary>
    public SectorAlignedMemory(int level = default)
    {
        this.level = level;
        // Assume ctor is called for allocation and leave Free unset
    }

    /// <summary>
    /// Create new instance of SectorAlignedMemory
    /// </summary>
    public SectorAlignedMemory(int numRecords, int sectorSize)
    {
        int recordSize = 1;
        int requiredSize = sectorSize + ((numRecords * recordSize + (sectorSize - 1)) & ~(sectorSize - 1));

        Buffer = GC.AllocateArray<byte>(requiredSize, true);
        long bufferAddr = (long)Unsafe.AsPointer(ref Buffer[0]);
        AlignedPointer = (byte*)((bufferAddr + (sectorSize - 1)) & ~((long)sectorSize - 1));
        Offset = (int)((long)AlignedPointer - bufferAddr);
        // Assume ctor is called for allocation and leave Free unset
    }

    /// <summary>
    /// Dispose
    /// </summary>
    public void Dispose()
    {
        Buffer = null;
#if CHECK_FREE
        Free = true;
#endif
    }

    /// <summary>
    /// Return
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Return()
    {
        pool?.Return(this);
    }

    /// <summary>
    /// Get valid pointer
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* GetValidPointer()
    {
        return AlignedPointer + ValidOffset;
    }

    /// <summary>
    /// ToString
    /// </summary>
    public override string ToString() => $"{(long)AlignedPointer} {Offset} {ValidOffset} {RequiredBytes} {AvailableBytes}"
#if CHECK_FREE
            + $" {Free}"
#endif
    ;
}

/// <summary>
/// SectorAlignedBufferPool is a pool of memory. 
/// Internally, it is organized as an array of concurrent queues where each concurrent
/// queue represents a memory of size in particular range. queue[i] contains memory 
/// segments each of size (2^i * sectorSize).
/// </summary>
public sealed class SectorAlignedBufferPool
{
    /// <summary>
    /// Disable buffer pool.
    /// This static option should be enabled on program entry, and not modified once Tsavorite is instantiated.
    /// </summary>
    public static bool Disabled;

    /// <summary>
    /// Unpin objects when they are returned to the pool, so that we do not hold pinned objects long term.
    /// If set, we will unpin when objects are returned and re-pin when objects are returned from the pool.
    /// This static option should be enabled on program entry, and not modified once Tsavorite is instantiated.
    /// </summary>
    public static bool UnpinOnReturn;

    private const int levels = 32;
    private readonly int recordSize;
    private readonly int sectorSize;
    private readonly ConcurrentQueue<SectorAlignedMemory>[] queue;
#if CHECK_FOR_LEAKS
    static int totalGets, totalReturns;
#endif

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="recordSize">Record size. May be 1 if allocations of different lengths will be made</param>
    /// <param name="sectorSize">Sector size, e.g. from log device</param>
    public SectorAlignedBufferPool(int recordSize, int sectorSize)
    {
        queue = new ConcurrentQueue<SectorAlignedMemory>[levels];
        this.recordSize = recordSize;
        this.sectorSize = sectorSize;
    }

    /// <summary>
    /// Return
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Return(SectorAlignedMemory page)
    {
#if CHECK_FOR_LEAKS
        Interlocked.Increment(ref totalReturns);
#endif

#if CHECK_FREE
        page.Free = true;
#endif // CHECK_FREE

        Debug.Assert(queue[page.Level] != null);
        page.AvailableBytes = 0;
        page.RequiredBytes = 0;
        page.ValidOffset = 0;
        Array.Clear(page.Buffer, 0, page.Buffer.Length);
        if (!Disabled)
        {
            if (UnpinOnReturn)
            {
                page.Handle.Free();
                page.Handle = default;
            }
            queue[page.Level].Enqueue(page);
        }
        else
        {
            if (UnpinOnReturn)
                page.Handle.Free();
            page.Buffer = null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Position(int v)
    {
        if (v == 1) return 0;
        return BitOperations.Log2((uint)v - 1) + 1;
    }

    /// <summary>
    /// Get buffer
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public unsafe SectorAlignedMemory Get(int numRecords)
    {
#if CHECK_FOR_LEAKS
        Interlocked.Increment(ref totalGets);
#endif

        int requiredSize = sectorSize + ((numRecords * recordSize + (sectorSize - 1)) & ~(sectorSize - 1));
        int index = Position(requiredSize / sectorSize);
        if (queue[index] == null)
        {
            var localPool = new ConcurrentQueue<SectorAlignedMemory>();
            Interlocked.CompareExchange(ref queue[index], localPool, null);
        }

        if (!Disabled && queue[index].TryDequeue(out SectorAlignedMemory page))
        {
#if CHECK_FREE
            page.Free = false;
#endif // CHECK_FREE
            if (UnpinOnReturn)
            {
                page.Handle = GCHandle.Alloc(page.Buffer, GCHandleType.Pinned);
                page.AlignedPointer = (byte*)(((long)page.Handle.AddrOfPinnedObject() + (sectorSize - 1)) & ~((long)sectorSize - 1));
                page.Offset = (int)((long)page.AlignedPointer - page.Handle.AddrOfPinnedObject());
            }
            return page;
        }

        page = new SectorAlignedMemory(level: index)
        {
            Buffer = GC.AllocateArray<byte>(sectorSize * (1 << index), !UnpinOnReturn)
        };
        if (UnpinOnReturn)
            page.Handle = GCHandle.Alloc(page.Buffer, GCHandleType.Pinned);
        long pageAddr = (long)Unsafe.AsPointer(ref page.Buffer[0]);
        page.AlignedPointer = (byte*)((pageAddr + (sectorSize - 1)) & ~((long)sectorSize - 1));
        page.Offset = (int)((long)page.AlignedPointer - pageAddr);
        page.pool = this;
        return page;
    }

    /// <summary>
    /// Free buffer
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Free()
    {
#if CHECK_FOR_LEAKS
        Debug.Assert(totalGets == totalReturns);
#endif
        for (int i = 0; i < levels; i++)
        {
            if (queue[i] == null) continue;
            while (queue[i].TryDequeue(out SectorAlignedMemory result))
                result.Buffer = null;
        }
    }

    /// <summary>
    /// Print pool contents
    /// </summary>
    public void Print()
    {
        for (int i = 0; i < levels; i++)
        {
            if (queue[i] == null) continue;
            foreach (SectorAlignedMemory item in queue[i])
            {
                Console.WriteLine("  " + item.ToString());
            }
        }
    }
}