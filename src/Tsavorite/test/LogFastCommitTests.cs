// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Tsavorite.Tests;

[TestFixture]
internal class LogFastCommitTests : TsavoriteLogTestBase
{
    [SetUp]
    public void Setup() => BaseSetup(false);

    [TearDown]
    public void TearDown() => BaseTearDown();

    [Test]
    [Category("TsavoriteLog")]
    [Category("Smoke")]
    public void TsavoriteLogSimpleFastCommitTest([Values] TestUtils.DeviceType deviceType)
    {
        byte[] cookie = new byte[100];
        new Random().NextBytes(cookie);

        string filename = Path.Join(TestUtils.MethodTestDir, $"fastCommit{deviceType}.log");
        device = TestUtils.CreateTestDevice(deviceType, filename, deleteOnClose: true);
        var logSettings = new TsavoriteLogSettings { LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager, FastCommitMode = true, TryRecoverLatest = false, SegmentSizeBits = 26 };
        log = new TsavoriteLog(logSettings);

        byte[] entry = new byte[entryLength];
        for (int i = 0; i < entryLength; i++)
            entry[i] = (byte)i;

        for (int i = 0; i < numEntries; i++)
        {
            log.Enqueue(entry);
        }

        byte[] cookie1 = new byte[100];
        new Random().NextBytes(cookie1);
        bool commitSuccessful = log.CommitStrongly(out long commit1Addr, out _, true, cookie1, 1);
        Assert.IsTrue(commitSuccessful);

        for (int i = 0; i < numEntries; i++)
        {
            log.Enqueue(entry);
        }

        byte[] cookie2 = new byte[100];
        new Random().NextBytes(cookie2);
        commitSuccessful = log.CommitStrongly(out long commit2Addr, out _, true, cookie2, 2);
        Assert.IsTrue(commitSuccessful);

        for (int i = 0; i < numEntries; i++)
        {
            log.Enqueue(entry);
        }

        byte[] cookie6 = new byte[100];
        new Random().NextBytes(cookie6);
        commitSuccessful = log.CommitStrongly(out long commit6Addr, out _, true, cookie6, 6);
        Assert.IsTrue(commitSuccessful);

        // Wait for all metadata writes to be complete to avoid a concurrent access exception
        log.Dispose();
        log = null;

        // be a deviant and remove commit metadata files
        manager.RemoveAllCommits();

        // Recovery should still work
        var recoveredLog = new TsavoriteLog(logSettings);
        recoveredLog.Recover(1);
        Assert.AreEqual(cookie1, recoveredLog.RecoveredCookie);
        Assert.AreEqual(commit1Addr, recoveredLog.TailAddress);
        recoveredLog.Dispose();

        recoveredLog = new TsavoriteLog(logSettings);
        recoveredLog.Recover(2);
        Assert.AreEqual(cookie2, recoveredLog.RecoveredCookie);
        Assert.AreEqual(commit2Addr, recoveredLog.TailAddress);
        recoveredLog.Dispose();

        // Default argument should recover to most recent, if TryRecoverLatest is set
        logSettings.TryRecoverLatest = true;
        recoveredLog = new TsavoriteLog(logSettings);
        Assert.AreEqual(cookie6, recoveredLog.RecoveredCookie);
        Assert.AreEqual(commit6Addr, recoveredLog.TailAddress);
        recoveredLog.Dispose();
    }

    [Test]
    [Category("TsavoriteLog")]
    [Category("Smoke")]
    public void CommitRecordBoundedGrowthTest([Values] TestUtils.DeviceType deviceType)
    {
        byte[] cookie = new byte[100];
        new Random().NextBytes(cookie);

        string filename = Path.Join(TestUtils.MethodTestDir, $"boundedGrowth{deviceType}.log");
        device = TestUtils.CreateTestDevice(deviceType, filename, deleteOnClose: true);
        var logSettings = new TsavoriteLogSettings { LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager, FastCommitMode = true, SegmentSizeBits = 26 };
        log = new TsavoriteLog(logSettings);

        byte[] entry = new byte[entryLength];
        for (int i = 0; i < entryLength; i++)
            entry[i] = (byte)i;

        for (int i = 0; i < 5 * numEntries; i++)
            log.Enqueue(entry);

        // for comparison, insert some entries without any commit records
        long referenceTailLength = log.TailAddress;

        var enqueueDone = new ManualResetEventSlim();
        var commitThreads = new List<Thread>();
        // Make sure to not spin up too many commit threads, otherwise we might clog epochs and halt progress
        for (int i = 0; i < Math.Max(1, Environment.ProcessorCount / 2); i++)
        {
            commitThreads.Add(new Thread(() =>
            {
                // Otherwise, absolutely clog the commit pipeline
                while (!enqueueDone.IsSet)
                    log.Commit();
            }));
        }

        foreach (Thread t in commitThreads)
            t.Start();
        for (int i = 0; i < 5 * numEntries; i++)
        {
            log.Enqueue(entry);
        }
        enqueueDone.Set();

        foreach (Thread t in commitThreads)
            t.Join();


        // TODO: Hardcoded constant --- if this number changes in TsavoriteLogRecoverInfo, it needs to be updated here too
        int commitRecordSize = 44;
        long logTailGrowth = log.TailAddress - referenceTailLength;
        // Check that we are not growing the log more than one commit record per user entry
        Assert.IsTrue(logTailGrowth - referenceTailLength <= commitRecordSize * 5 * numEntries);

        // Ensure clean shutdown
        log.Commit(true);
    }
}