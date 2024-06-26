﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using NUnit.Framework;
using static Tsavorite.Utility;

namespace Tsavorite.Tests;

[TestFixture]
internal class SpanByteTests
{
    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public unsafe void SpanByteTest1()
    {
        Span<byte> output = stackalloc byte[20];
        SpanByte input = default;
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        try
        {
            using var log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "hlog1.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<SpanByte, SpanByte>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 });
            using ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>> s = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());

            ReadOnlySpan<byte> key1 = MemoryMarshal.Cast<char, byte>("key1".AsSpan());
            ReadOnlySpan<byte> value1 = MemoryMarshal.Cast<char, byte>("value1".AsSpan());
            var output1 = SpanByteAndMemory.FromPinnedSpan(output);

            fixed (byte* key1Ptr = key1)
            fixed (byte* value1Ptr = value1)
            {
                var key1SpanByte = SpanByte.FromPinnedPointer(key1Ptr, key1.Length);
                var value1SpanByte = SpanByte.FromPinnedPointer(value1Ptr, value1.Length);

                s.Upsert(key1SpanByte, value1SpanByte);
                s.Read(ref key1SpanByte, ref input, ref output1);
            }

            Assert.IsTrue(output1.IsSpanByte);
            Assert.IsTrue(output1.SpanByte.AsReadOnlySpan().SequenceEqual(value1));

            ReadOnlySpan<byte> key2 = MemoryMarshal.Cast<char, byte>("key2".AsSpan());
            ReadOnlySpan<byte> value2 = MemoryMarshal.Cast<char, byte>("value2value2value2".AsSpan());
            var output2 = SpanByteAndMemory.FromPinnedSpan(output);

            fixed (byte* key2Ptr = key2)
            fixed (byte* value2Ptr = value2)
            {
                var key2SpanByte = SpanByte.FromPinnedPointer(key2Ptr, key2.Length);
                var value2SpanByte = SpanByte.FromPinnedPointer(value2Ptr, value2.Length);

                s.Upsert(key2SpanByte, value2SpanByte);
                s.Read(ref key2SpanByte, ref input, ref output2);
            }

            Assert.IsTrue(!output2.IsSpanByte);
            Assert.IsTrue(output2.Memory.Memory.Span.Slice(0, output2.Length).SequenceEqual(value2));
        }
        finally
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }
    }

    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public unsafe void MultiRead_SpanByte_Test()
    {
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        try
        {
            using var log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<SpanByte, SpanByte>(
                size: 1L << 10,
                new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });
            using ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>> session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());

            for (int i = 0; i < 200; i++)
            {
                ReadOnlySpan<byte> key = MemoryMarshal.Cast<char, byte>($"{i}".AsSpan());
                ReadOnlySpan<byte> value = MemoryMarshal.Cast<char, byte>($"{i + 1000}".AsSpan());
                fixed (byte* k = key, v = value)
                    session.Upsert(SpanByte.FromPinnedSpan(key), SpanByte.FromPinnedSpan(value));
            }

            // Read, evict all records to disk, read again
            MultiRead(evicted: false);
            store.Log.FlushAndEvict(true);
            MultiRead(evicted: true);

            void MultiRead(bool evicted)
            {
                for (long key = 0; key < 50; key++)
                {
                    // read each key multiple times
                    for (int i = 0; i < 10; i++)
                        ReadKey(key, key + 1000, evicted);
                }
            }

            void ReadKey(long key, long value, bool evicted)
            {
                Status status;
                SpanByteAndMemory output = default;

                ReadOnlySpan<byte> keyBytes = MemoryMarshal.Cast<char, byte>($"{key}".AsSpan());
                fixed (byte* _ = keyBytes)
                    status = session.Read(key: SpanByte.FromPinnedSpan(keyBytes), out output);
                Assert.AreEqual(evicted, status.IsPending, "evicted/pending mismatch");

                if (!evicted)
                    Assert.IsTrue(status.Found, $"expected to find key; status = {status}");
                else    // needs to be fetched from disk
                {
                    session.CompletePendingWithOutputs(out CompletedOutputIterator<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty> completedOutputs, wait: true);
                    using (completedOutputs)
                    {
                        for (int count = 0; completedOutputs.Next(); ++count)
                        {
                            Assert.AreEqual(0, count, "should only have one record returned");
                            Assert.IsTrue(completedOutputs.Current.Status.Found);
                            output = completedOutputs.Current.Output;
                        }
                    }
                }
                string outputString = new string(MemoryMarshal.Cast<byte, char>(output.Memory.Memory.Span));
                Assert.AreEqual(value, long.Parse(outputString));
            }
        }
        finally
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }
    }

    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public unsafe void SpanByteUnitTest1()
    {
        Span<byte> payload = stackalloc byte[20];
        Span<byte> serialized = stackalloc byte[24];

        SpanByte sb = SpanByte.FromPinnedSpan(payload);
        Assert.IsFalse(sb.Serialized);
        Assert.AreEqual(20, sb.Length);
        Assert.AreEqual(24, sb.TotalSize);
        Assert.AreEqual(20, sb.AsSpan().Length);
        Assert.AreEqual(20, sb.AsReadOnlySpan().Length);

        fixed (byte* ptr = serialized)
            sb.CopyTo(ptr);
        ref SpanByte ssb = ref SpanByte.ReinterpretWithoutLength(serialized);
        Assert.IsTrue(ssb.Serialized);
        Assert.AreEqual(0, ssb.MetadataSize);
        Assert.AreEqual(20, ssb.Length);
        Assert.AreEqual(24, ssb.TotalSize);
        Assert.AreEqual(20, ssb.AsSpan().Length);
        Assert.AreEqual(20, ssb.AsReadOnlySpan().Length);

        ssb.MarkExtraMetadata();
        Assert.IsTrue(ssb.Serialized);
        Assert.AreEqual(8, ssb.MetadataSize);
        Assert.AreEqual(20, ssb.Length);
        Assert.AreEqual(24, ssb.TotalSize);
        Assert.AreEqual(20 - 8, ssb.AsSpan().Length);
        Assert.AreEqual(20 - 8, ssb.AsReadOnlySpan().Length);
        ssb.ExtraMetadata = 31337;
        Assert.AreEqual(31337, ssb.ExtraMetadata);

        sb.MarkExtraMetadata();
        Assert.AreEqual(20, sb.Length);
        Assert.AreEqual(24, sb.TotalSize);
        Assert.AreEqual(20 - 8, sb.AsSpan().Length);
        Assert.AreEqual(20 - 8, sb.AsReadOnlySpan().Length);
        sb.ExtraMetadata = 31337;
        Assert.AreEqual(31337, sb.ExtraMetadata);

        fixed (byte* ptr = serialized)
            sb.CopyTo(ptr);
        Assert.IsTrue(ssb.Serialized);
        Assert.AreEqual(8, ssb.MetadataSize);
        Assert.AreEqual(20, ssb.Length);
        Assert.AreEqual(24, ssb.TotalSize);
        Assert.AreEqual(20 - 8, ssb.AsSpan().Length);
        Assert.AreEqual(20 - 8, ssb.AsReadOnlySpan().Length);
        Assert.AreEqual(31337, ssb.ExtraMetadata);
    }

    [Test]
    [Category("TsavoriteKV")]
    public unsafe void ShouldSkipEmptySpaceAtEndOfPage()
    {
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        using var log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "vl-iter.log"), deleteOnClose: true);
        using var store = new TsavoriteKV<SpanByte, SpanByte>
            (128,
            new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 10 }, // 1KB page
            null, null, null, concurrencyControlMode: ConcurrencyControlMode.None);
        using ClientSession<SpanByte, SpanByte, SpanByte, int[], Empty, VLVectorFunctions> session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());

        const int PageSize = 1024;
        Span<long> keySpan = stackalloc long[1];
        SpanByte key = keySpan.AsSpanByte();
        Span<byte> valueSpan = stackalloc byte[PageSize];
        SpanByte value = valueSpan.AsSpanByte();  // We'll adjust the length below

        Set(ref keySpan, 1L, ref valueSpan, 800, 1);                // Inserted on page#0 and leaves empty space
        Set(ref keySpan, 2L, ref valueSpan, 800, 2);                // Inserted on page#1 because there is not enough space in page#0, and leaves empty space

        // Add a second record on page#1 to fill it exactly. Page#1 starts at offset 0 on the page (unlike page#0, which starts at 24 or 64,
        // depending on data). Subtract the RecordInfo and key space for both the first record and the second record we're about to insert,
        // the value space for the first record, and the length header for the second record. This is the space available for the second record's value.
        int p2value2len = PageSize
                            - 2 * RecordInfo.GetLength()
                            - 2 * RoundUp(key.TotalSize, SpanByteAllocator.RecordAlignment)
                            - RoundUp(value.TotalSize, SpanByteAllocator.RecordAlignment)
                            - sizeof(int);
        Set(ref keySpan, 3L, ref valueSpan, p2value2len, 3);        // Inserted on page#1
        Assert.AreEqual(PageSize * 2, store.Log.TailAddress, "TailAddress should be at the end of page#2");

        Set(ref keySpan, 4L, ref valueSpan, 64, 4);                 // Inserted on page#2

        var data = new List<(long, int, int)>();
        using (ITsavoriteScanIterator<SpanByte, SpanByte> iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
        {
            while (iterator.GetNext(out RecordInfo info))
            {
                Span<long> scanKey = iterator.GetKey().AsSpan<long>();
                Span<byte> scanValue = iterator.GetValue().AsSpan<byte>();

                data.Add((scanKey[0], scanValue.Length, scanValue[0]));
            }
        }

        Assert.AreEqual(4, data.Count);

        Assert.AreEqual((1L, 800, 1), data[0]);
        Assert.AreEqual((2L, 800, 2), data[1]);
        Assert.AreEqual((3L, p2value2len, 3), data[2]);
        Assert.AreEqual((4L, 64, 4), data[3]);

        TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

        void Set(ref Span<long> keySpan, long keyValue, ref Span<byte> valueSpan, int valueLength, byte tag)
        {
            keySpan[0] = keyValue;
            value.Length = valueLength;
            valueSpan[0] = tag;
            session.Upsert(ref key, ref value, Empty.Default, 0);
        }
    }
}