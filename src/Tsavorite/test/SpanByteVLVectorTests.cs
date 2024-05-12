// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using static Tsavorite.Tests.TestUtils;

namespace Tsavorite.Tests;

[TestFixture]
internal class SpanByteVLVectorTests
{
    private const int StackAllocMax = 12;

    private static int GetRandomLength(Random r) => r.Next(StackAllocMax) + 1;    // +1 for 0 to StackAllocMax inclusive

    [Test]
    [Category(TsavoriteKVTestCategory)]
    [Category(SmokeTestCategory)]
    public unsafe void VLVectorSingleKeyTest()
    {
        DeleteDirectory(MethodTestDir, wait: true);

        var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog1.log"), deleteOnClose: true);
        var store = new TsavoriteKV<SpanByte, SpanByte>
            (128,
            new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 },
            null, null, null);
        ClientSession<SpanByte, SpanByte, SpanByte, int[], Empty, VLVectorFunctions> s = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());

        // Single alloc outside the loop, to the max length we'll need.
        Span<int> keySpan = stackalloc int[1];
        Span<int> valueSpan = stackalloc int[StackAllocMax];

        Random rng = new(100);
        for (int i = 0; i < 5000; i++)
        {
            keySpan[0] = i;
            SpanByte keySpanByte = keySpan.AsSpanByte();

            int len = GetRandomLength(rng);
            for (int j = 0; j < len; j++)
                valueSpan[j] = len;
            SpanByte valueSpanByte = valueSpan.Slice(0, len).AsSpanByte();

            s.Upsert(ref keySpanByte, ref valueSpanByte, Empty.Default, 0);
        }

        // Reset rng to get the same sequence of value lengths
        rng = new Random(100);
        for (int i = 0; i < 5000; i++)
        {
            keySpan[0] = i;
            SpanByte keySpanByte = keySpan.AsSpanByte();

            int valueLen = GetRandomLength(rng);
            int[] output = null;
            Status status = s.Read(ref keySpanByte, ref output, Empty.Default, 0);

            if (status.IsPending)
            {
                s.CompletePendingWithOutputs(out CompletedOutputIterator<SpanByte, SpanByte, SpanByte, int[], Empty> outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }

            Assert.IsTrue(status.Found);
            Assert.AreEqual(valueLen, output.Length);
            for (int j = 0; j < valueLen; j++)
                Assert.AreEqual(valueLen, output[j]);
        }
        s.Dispose();
        store.Dispose();
        log.Dispose();
        DeleteDirectory(MethodTestDir);
    }

    [Test]
    [Category(TsavoriteKVTestCategory)]
    [Category(SmokeTestCategory)]
    public unsafe void VLVectorMultiKeyTest()
    {
        DeleteDirectory(MethodTestDir, wait: true);

        var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog1.log"), deleteOnClose: true);
        var store = new TsavoriteKV<SpanByte, SpanByte>
            (128,
            new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 },
            null, null, null);
        ClientSession<SpanByte, SpanByte, SpanByte, int[], Empty, VLVectorFunctions> s = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());

        // Single alloc outside the loop, to the max length we'll need.
        Span<int> keySpan = stackalloc int[StackAllocMax];
        Span<int> valueSpan = stackalloc int[StackAllocMax];

        Random rng = new(100);
        for (int i = 0; i < 5000; i++)
        {
            int keyLen = GetRandomLength(rng);
            for (int j = 0; j < keyLen; j++)
                keySpan[j] = i;
            SpanByte keySpanByte = keySpan.AsSpanByte();

            int valueLen = GetRandomLength(rng);
            for (int j = 0; j < valueLen; j++)
                valueSpan[j] = valueLen;
            SpanByte valueSpanByte = valueSpan.Slice(0, valueLen).AsSpanByte();

            s.Upsert(ref keySpanByte, ref valueSpanByte, Empty.Default, 0);
        }

        // Reset rng to get the same sequence of key and value lengths
        rng = new Random(100);
        for (int i = 0; i < 5000; i++)
        {
            int keyLen = GetRandomLength(rng);
            for (int j = 0; j < keyLen; j++)
                keySpan[j] = i;
            SpanByte keySpanByte = keySpan.AsSpanByte();

            int valueLen = GetRandomLength(rng);
            int[] output = null;
            Status status = s.Read(ref keySpanByte, ref output, Empty.Default, 0);

            if (status.IsPending)
            {
                s.CompletePendingWithOutputs(out CompletedOutputIterator<SpanByte, SpanByte, SpanByte, int[], Empty> outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }

            Assert.IsTrue(status.Found);
            Assert.AreEqual(valueLen, output.Length);
            for (int j = 0; j < valueLen; j++)
                Assert.AreEqual(valueLen, output[j]);
        }

        s.Dispose();
        store.Dispose();
        log.Dispose();
        DeleteDirectory(MethodTestDir);
    }
}