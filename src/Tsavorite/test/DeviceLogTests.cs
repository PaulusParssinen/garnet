﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using NUnit.Framework;
using Tsavorite.Device;

namespace Tsavorite.Tests;

[TestFixture]
internal class DeviceLogTests
{
    private const int entryLength = 100;
    private const int numEntries = 1000;
    private TsavoriteLog log;
    private static readonly byte[] entry = new byte[100];

    [Test]
    [Category("TsavoriteLog")]
    public void BasicHighLatencyDeviceTest()
    {
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        // Create devices \ log for test for in memory device
        using var device = new LocalMemoryDevice(1L << 28, 1L << 25, 2, latencyMs: 20, fileName: Path.Join(TestUtils.MethodTestDir, "test.log"));
        using var LocalMemorylog = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 80, MemorySizeBits = 20, GetMemory = null, SegmentSizeBits = 80, MutableFraction = 0.2, LogCommitManager = null });

        int entryLength = 10;

        // Set Default entry data
        for (int i = 0; i < entryLength; i++)
        {
            entry[i] = (byte)i;
            LocalMemorylog.Enqueue(entry);
        }

        // Commit to the log
        LocalMemorylog.Commit(true);

        // Read the log just to verify was actually committed
        int currentEntry = 0;
        using TsavoriteLogScanIterator iter = LocalMemorylog.Scan(0, 100_000_000);
        while (iter.GetNext(out byte[] result, out _, out _))
        {
            Assert.IsTrue(result[currentEntry] == currentEntry, "Fail - Result[" + currentEntry.ToString() + "]: is not same as " + currentEntry.ToString());
            currentEntry++;
        }
    }

    private async ValueTask TsavoriteLogTest1(LogChecksumType logChecksum, IDevice device, ILogCommitManager logCommitManager, TsavoriteLogTestBase.IteratorType iteratorType)
    {
        var logSettings = new TsavoriteLogSettings { PageSizeBits = 20, SegmentSizeBits = 20, LogDevice = device, LogChecksum = logChecksum, LogCommitManager = logCommitManager, TryRecoverLatest = false };
        log = TsavoriteLogTestBase.IsAsync(iteratorType) ? await TsavoriteLog.CreateAsync(logSettings) : new TsavoriteLog(logSettings);

        byte[] entry = new byte[entryLength];
        for (int i = 0; i < entryLength; i++)
            entry[i] = (byte)i;

        for (int i = 0; i < numEntries; i++)
        {
            log.Enqueue(entry);
        }

        log.CompleteLog(true);

        // MoveNextAsync() would hang at TailAddress, waiting for more entries (that we don't add).
        // Note: If this happens and the test has to be canceled, there may be a leftover blob from the log.Commit(), because
        // the log device isn't Dispose()d; the symptom is currently a numeric string format error in DefaultCheckpointNamingScheme.
        using (TsavoriteLogScanIterator iter = log.Scan(0, long.MaxValue))
        {
            var counter = new TsavoriteLogTestBase.Counter(log);

            switch (iteratorType)
            {
                case TsavoriteLogTestBase.IteratorType.AsyncByteVector:
                    await foreach ((byte[] result, _, _, long nextAddress) in iter.GetAsyncEnumerable())
                    {
                        Assert.IsTrue(result.SequenceEqual(entry));
                        counter.IncrementAndMaybeTruncateUntil(nextAddress);
                    }
                    break;
                case TsavoriteLogTestBase.IteratorType.AsyncMemoryOwner:
                    await foreach ((IMemoryOwner<byte> result, int _, long _, long nextAddress) in iter.GetAsyncEnumerable(MemoryPool<byte>.Shared))
                    {
                        Assert.IsTrue(result.Memory.Span.ToArray().Take(entry.Length).SequenceEqual(entry));
                        result.Dispose();
                        counter.IncrementAndMaybeTruncateUntil(nextAddress);
                    }
                    break;
                case TsavoriteLogTestBase.IteratorType.Sync:
                    while (iter.GetNext(out byte[] result, out _, out _))
                    {
                        Assert.IsTrue(result.SequenceEqual(entry));
                        counter.IncrementAndMaybeTruncateUntil(iter.NextAddress);
                    }
                    break;
                default:
                    Assert.Fail("Unknown IteratorType");
                    break;
            }
            Assert.IsTrue(counter.count == numEntries);
        }

        log.Dispose();
    }
}