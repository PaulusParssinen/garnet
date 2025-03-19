﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    internal sealed unsafe class BlittableAllocatorImpl<TKey, TValue, TStoreFunctions> : AllocatorBase<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
    {
        /// <summary>
        /// Circular buffer of <see cref="AllocatorBase{TKey, TValue, TStoreFunctions, TAllocator}.BufferSize"/> memory buffers 
        /// with each memory buffer being size of <see cref="AllocatorBase{TKey, TValue, TStoreFunctions, TAllocator}.PageSize"/>
        /// </summary>
        private readonly byte** pointers;

        private static int KeySize => Unsafe.SizeOf<TKey>();
        private static int ValueSize => Unsafe.SizeOf<TValue>();
        internal static int RecordSize => Unsafe.SizeOf<AllocatorRecord<TKey, TValue>>();

        private readonly OverflowPool<PageUnit> overflowPagePool;

        public BlittableAllocatorImpl(AllocatorSettings settings, TStoreFunctions storeFunctions, Func<object, BlittableAllocator<TKey, TValue, TStoreFunctions>> wrapperCreator)
            : base(settings.LogSettings, storeFunctions, wrapperCreator, settings.evictCallback, settings.epoch, settings.flushCallback, settings.logger)
        {
            if (!Utility.IsBlittable<TKey>() || !Utility.IsBlittable<TValue>())
                throw new TsavoriteException($"BlittableAllocator requires blittlable Key ({typeof(TKey)}) and Value ({typeof(TValue)})");

            overflowPagePool = new OverflowPool<PageUnit>(4, static p => {
                NativeMemory.AlignedFree(p.Pointer);
                GC.RemoveMemoryPressure(p.Size);
            });

            if (BufferSize > 0)
            {
                pointers = (byte**)NativeMemory.AllocZeroed((uint)BufferSize, (uint)sizeof(byte*));
            }
        }

        public override void Reset()
        {
            base.Reset();
            for (int index = 0; index < BufferSize; index++)
            {
                if (IsAllocated(index))
                    FreePage(index);
            }
            Initialize();
        }

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            var pagePtr = pointers[index];
            if (pagePtr != null)
            {
                _ = overflowPagePool.TryAdd(new PageUnit
                {
                    Pointer = pagePtr,
                    Size = PageSize
                });
                pointers[index] = null;

                _ = Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        public override void Initialize() => Initialize(Constants.kFirstValidAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfo(long physicalAddress) => ref Unsafe.AsRef<RecordInfo>((void*)physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfoFromBytePointer(byte* ptr) => ref Unsafe.AsRef<RecordInfo>(ptr);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref TKey GetKey(long physicalAddress) => ref Unsafe.AsRef<TKey>((byte*)physicalAddress + RecordInfo.GetLength());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref TValue GetValue(long physicalAddress) => ref Unsafe.AsRef<TValue>((byte*)physicalAddress + RecordInfo.GetLength() + KeySize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (int actualSize, int allocatedSize) GetRecordSize(long physicalAddress) => (RecordSize, RecordSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static (int actualSize, int allocatedSize, int keySize) GetRMWCopyDestinationRecordSize<TInput, TVariableLengthInput>(ref TKey key, ref TInput input, ref TValue value, ref RecordInfo recordInfo, TVariableLengthInput varlenInput)
            => (RecordSize, RecordSize, KeySize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (int actualSize, int allocatedSize, int keySize) GetRMWInitialRecordSize<TInput, TSessionFunctionsWrapper>(ref TKey key, ref TInput input, TSessionFunctionsWrapper sessionFunctions)
            => (RecordSize, RecordSize, KeySize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetRequiredRecordSize(long physicalAddress, int availableBytes) => GetAverageRecordSize();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetAverageRecordSize() => RecordSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetFixedRecordSize() => RecordSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref TKey key, ref TValue value) => (RecordSize, RecordSize, KeySize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (int actualSize, int allocatedSize, int keySize) GetUpsertRecordSize<TInput, TSessionFunctionsWrapper>(ref TKey key, ref TValue value, ref TInput input, TSessionFunctionsWrapper sessionFunctions)
            => (RecordSize, RecordSize, KeySize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetValueLength(ref TValue value) => ValueSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SerializeKey(ref TKey src, long physicalAddress) => GetKey(physicalAddress) = src;

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            overflowPagePool.Dispose();
        }

        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        internal void AllocatePage(int index)
        {
            IncrementAllocatedPageCount();

            if (overflowPagePool.TryGet(out var item))
            {
                pointers[index] = item.Pointer;
                return;
            }

            pointers[index] = (byte*)NativeMemory.AlignedAlloc((uint)PageSize, alignment: (uint)sectorSize);
            GC.AddMemoryPressure(PageSize);
            ClearPage(index, 0);
        }

        internal int OverflowPageCount => overflowPagePool.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddress(long logicalAddress)
        {
            // Offset within page
            var offset = (int)(logicalAddress & ((1L << LogPageSizeBits) - 1));

            // Index of page within the circular buffer
            var pageIndex = (int)((logicalAddress >> LogPageSizeBits) & (BufferSize - 1));
            return (long)(pointers[pageIndex] + offset);
        }

        internal bool IsAllocated(int pageIndex) => pointers[pageIndex] != null;

        protected override void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                    (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)AlignedPageSizeBytes,
                    callback,
                    asyncResult, device);
        }

        protected override void WriteAsyncToDevice<TContext>
            (long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long[] localSegmentOffsets, long fuzzyStartLogicalAddress)
        {
            VerifyCompatibleSectorSize(device);
            var alignedPageSize = (pageSize + (sectorSize - 1)) & ~(sectorSize - 1);

            WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                        (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                        (uint)alignedPageSize, callback, asyncResult,
                        device);
        }

        /// <summary>
        /// Get start logical address
        /// </summary>
        public long GetStartLogicalAddress(long page) => page << LogPageSizeBits;

        /// <summary>
        /// Get first valid logical address
        /// </summary>
        public long GetFirstValidLogicalAddress(long page)
        {
            if (page == 0)
                return (page << LogPageSizeBits) + Constants.kFirstValidAddress;
            return page << LogPageSizeBits;
        }

        internal void ClearPage(long page, int offset)
        {
            Debug.Assert(offset < PageSize);
            Debug.Assert(IsAllocated(GetPageIndexForPage(page)));

            var ptr = pointers[page % BufferSize] + offset;
            var length = (uint)(PageSize - offset);

            NativeMemory.Clear(ptr, length);
        }

        internal void FreePage(long page)
        {
            ClearPage(page, 0);
            if (EmptyPageCount > 0)
                ReturnPage(GetPageIndexForPage(page));
        }

        /// <summary>
        /// Delete in-memory portion of the log
        /// </summary>
        internal override void DeleteFromMemory()
        {
            for (var i = 0; i < BufferSize; i++)
            {
                var pagePtr = pointers[i];
                if (pagePtr != null)
                {
                    NativeMemory.AlignedFree(pagePtr);
                    GC.RemoveMemoryPressure(PageSize);
                    pointers[i] = null;
                }
            }
        }

        protected override void ReadAsync<TContext>(ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
                DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
            => device.ReadAsync(alignedSourceAddress, (IntPtr)pointers[destinationPageIndex], aligned_read_length, callback, asyncResult);

        /// <summary>
        /// Invoked by users to obtain a record from disk. It uses sector aligned memory to read 
        /// the record efficiently into memory.
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<TKey, TValue> context, SectorAlignedMemory result = default)
            => throw new InvalidOperationException("AsyncReadRecordObjectsToMemory invalid for BlittableAllocator");

        internal static bool RetrievedFullRecord(byte* record, ref AsyncIOContext<TKey, TValue> ctx)
        {
            ctx.key = GetKey((long)record);
            ctx.value = GetValue((long)record);
            return true;
        }

        internal static long[] GetSegmentOffsets() => null;

        internal static void PopulatePage(byte* src, int required_bytes, long destinationPage)
            => throw new TsavoriteException("BlittableAllocator memory pages are sector aligned - use direct copy");

        /// <summary>
        /// Iterator interface for pull-scanning Tsavorite log
        /// </summary>
        public override ITsavoriteScanIterator<TKey, TValue> Scan(TsavoriteKV<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> store,
                long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, bool includeSealedRecords)
            => new BlittableScanIterator<TKey, TValue, TStoreFunctions>(store, this, beginAddress, endAddress, scanBufferingMode, includeSealedRecords, epoch, logger: logger);

        /// <summary>
        /// Implementation for push-scanning Tsavorite log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(TsavoriteKV<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> store,
                long beginAddress, long endAddress, ref TScanFunctions scanFunctions, ScanBufferingMode scanBufferingMode)
        {
            using BlittableScanIterator<TKey, TValue, TStoreFunctions> iter = new(store, this, beginAddress, endAddress, scanBufferingMode, false, epoch, logger: logger);
            return PushScanImpl(beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-scanning Tsavorite log with a cursor, called from LogAccessor
        /// </summary>
        internal override bool ScanCursor<TScanFunctions>(TsavoriteKV<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> store,
                ScanCursorState<TKey, TValue> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress)
        {
            using BlittableScanIterator<TKey, TValue, TStoreFunctions> iter = new(store, this, cursor, endAddress, ScanBufferingMode.SinglePageBuffering, false, epoch, logger: logger);
            return ScanLookup<long, long, TScanFunctions, BlittableScanIterator<TKey, TValue, TStoreFunctions>>(store, scanCursorState, ref cursor, count, scanFunctions, iter, validateCursor, maxAddress);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> store, ref TKey key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using BlittableScanIterator<TKey, TValue, TStoreFunctions> iter = new(store, this, beginAddress, epoch, logger: logger);
            return IterateKeyVersionsImpl(store, ref key, beginAddress, ref scanFunctions, iter);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator<TKey, TValue>> observer)
        {
            using var iter = new BlittableScanIterator<TKey, TValue, TStoreFunctions>(store: null, this, beginAddress, endAddress, ScanBufferingMode.NoBuffering, false, epoch, true, logger: logger);
            observer?.OnNext(iter);
        }

        /// <summary>
        /// Read pages from specified device
        /// </summary>
        internal void AsyncReadPagesFromDeviceToFrame<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
                                        BlittableFrame frame,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null,
                                        IDevice objectLogDevice = null,
                                        CancellationTokenSource cts = null)
        {
            var usedDevice = device ?? this.device;

            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                int pageIndex = (int)(readPage % frame.frameSize);
                if (frame.frame[pageIndex] == null)
                    frame.Allocate(pageIndex);
                else
                    frame.Clear(pageIndex);

                var asyncResult = new PageAsyncReadResult<TContext>()
                {
                    page = readPage,
                    context = context,
                    handle = completed,
                    frame = frame,
                    cts = cts
                };

                ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);

                uint readLength = (uint)AlignedPageSizeBytes;
                long adjustedUntilAddress = (AlignedPageSizeBytes * (untilAddress >> LogPageSizeBits) + (untilAddress & PageSizeMask));

                if (adjustedUntilAddress > 0 && ((adjustedUntilAddress - (long)offsetInFile) < PageSize))
                {
                    readLength = (uint)(adjustedUntilAddress - (long)offsetInFile);
                    readLength = (uint)((readLength + (sectorSize - 1)) & ~(sectorSize - 1));
                }

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                usedDevice.ReadAsync(offsetInFile, (IntPtr)frame.pointers[pageIndex], readLength, callback, asyncResult);
            }
        }
    }
}