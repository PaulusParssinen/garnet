﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using NUnit.Framework;

namespace Tsavorite.Tests.Recovery;

[TestFixture]
public class LogRecoverReadOnlyTests
{
    private const int ProducerPauseMs = 1;
    private const int CommitPeriodMs = 20;
    private const int RestorePeriodMs = 5;
    private const int NumElements = 100;
    private string deviceName;
    private CancellationTokenSource cts;
    private SemaphoreSlim done;

    [SetUp]
    public void Setup()
    {
        deviceName = Path.Join(TestUtils.MethodTestDir, "testlog");

        // Clean up log files from previous test runs in case they weren't cleaned up
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        cts = new CancellationTokenSource();
        done = new SemaphoreSlim(0);
    }

    [TearDown]
    public void TearDown()
    {
        cts?.Dispose();
        cts = default;
        done?.Dispose();
        done = default;
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
    }

    [Test]
    [Category("TsavoriteLog")]
    public async Task RecoverReadOnlyCheck1([Values] bool isAsync)
    {
        using var device = Devices.CreateLogDevice(deviceName);
        var logSettings = new TsavoriteLogSettings { LogDevice = device, MemorySizeBits = 11, PageSizeBits = 9, MutableFraction = 0.5, SegmentSizeBits = 9, TryRecoverLatest = false };
        using TsavoriteLog log = isAsync ? await TsavoriteLog.CreateAsync(logSettings) : new TsavoriteLog(logSettings);

        await Task.WhenAll(ProducerAsync(log, cts),
                           CommitterAsync(log, cts.Token),
                           ReadOnlyConsumerAsync(deviceName, isAsync, cts.Token));
    }

    private async Task ProducerAsync(TsavoriteLog log, CancellationTokenSource cts)
    {
        for (long i = 0L; i < NumElements; ++i)
        {
            log.Enqueue(Encoding.UTF8.GetBytes(i.ToString()));
            await Task.Delay(TimeSpan.FromMilliseconds(ProducerPauseMs));
        }
        // Ensure the reader had time to see all data
        await done.WaitAsync();
        cts.Cancel();
    }

    private static async Task CommitterAsync(TsavoriteLog log, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(CommitPeriodMs), cancellationToken);
                await log.CommitAsync(token: cancellationToken);
            }
        }
        catch (OperationCanceledException) { }
    }

    // This creates a separate TsavoriteLog over the same log file, using RecoverReadOnly to continuously update
    // to the primary TsavoriteLog's commits.
    private async Task ReadOnlyConsumerAsync(string deviceName, bool isAsync, CancellationToken cancellationToken)
    {
        using var device = Devices.CreateLogDevice(deviceName);
        var logSettings = new TsavoriteLogSettings { LogDevice = device, ReadOnlyMode = true, PageSizeBits = 9, SegmentSizeBits = 9 };
        using TsavoriteLog log = isAsync ? await TsavoriteLog.CreateAsync(logSettings, cancellationToken) : new TsavoriteLog(logSettings);

        Task _ = BeginRecoverAsyncLoop();

        // This enumerator waits asynchronously when we have reached the committed tail of the duplicate TsavoriteLog. When RecoverReadOnly
        // reads new data committed by the primary TsavoriteLog, it signals commit completion to let iter continue to the new tail.
        using TsavoriteLogScanIterator iter = log.Scan(log.BeginAddress, long.MaxValue);
        long prevValue = -1L;
        try
        {
            await foreach ((byte[] result, int _, long _, long nextAddress) in iter.GetAsyncEnumerable(cancellationToken))
            {
                long value = long.Parse(Encoding.UTF8.GetString(result));
                Assert.AreEqual(prevValue + 1, value);
                prevValue = value;
                iter.CompleteUntil(nextAddress);
                if (prevValue == NumElements - 1)
                    done.Release();
            }
        }
        catch (OperationCanceledException) { }
        Assert.AreEqual(NumElements - 1, prevValue);

        async Task BeginRecoverAsyncLoop()
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Delay for a while before recovering to the last commit by the primary TsavoriteLog instance.
                await Task.Delay(TimeSpan.FromMilliseconds(RestorePeriodMs), cancellationToken);
                if (cancellationToken.IsCancellationRequested)
                    break;
                long startTime = DateTimeOffset.UtcNow.Ticks;
                while (true)
                {
                    try
                    {
                        if (isAsync)
                        {
                            await log.RecoverReadOnlyAsync(cancellationToken);
                        }
                        else
                            log.RecoverReadOnly();
                        break;
                    }
                    catch
                    { }
                    Thread.Yield();
                    // retry until timeout
                    if (DateTimeOffset.UtcNow.Ticks - startTime > TimeSpan.FromSeconds(5).Ticks)
                        throw new Exception("Timed out retrying RecoverReadOnly");
                }
            }
        }
    }
}