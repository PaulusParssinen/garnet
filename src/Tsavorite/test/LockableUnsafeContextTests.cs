﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using NUnit.Framework;
using Tsavorite.Tests.LockTable;
using Tsavorite.Tests.ReadCacheTests;
using static Tsavorite.Tests.TestUtils;

namespace Tsavorite.Tests.LockableUnsafeContext;

// Functions for the "Simple lock transaction" case, e.g.:
//  - Lock key1, key2, key3, keyResult
//  - Do some operation on value1, value2, value3 and write the result to valueResult
internal class LockableUnsafeFunctions : SimpleFunctions<long, long>
{
    internal long recordAddress;

    public override void PostSingleDeleter(ref long key, ref DeleteInfo deleteInfo)
    {
        recordAddress = deleteInfo.Address;
    }

    public override bool ConcurrentDeleter(ref long key, ref long value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
    {
        recordAddress = deleteInfo.Address;
        return true;
    }
}

internal class LockableUnsafeComparer : ITsavoriteEqualityComparer<long>
{
    internal int maxSleepMs;
    private readonly Random rng = new(101);

    public bool Equals(ref long k1, ref long k2) => k1 == k2;

    public long GetHashCode64(ref long k)
    {
        if (maxSleepMs > 0)
            Thread.Sleep(rng.Next(maxSleepMs));
        return Utility.GetHashCode(k);
    }
}

public enum ResultLockTarget { MutableLock, LockTable }

internal struct BucketLockTracker
{
    internal readonly Dictionary<long /* bucketIndex */, (int x, int s)> buckets;

    public BucketLockTracker()
    {
        buckets = new();
    }

    internal void Increment(FixedLengthLockableKeyStruct<long> key) => Increment(ref key); // easier with 'foreach' because iteration vars can't be passed by 'ref'
    internal void Increment(ref FixedLengthLockableKeyStruct<long> key)
    {
        if (key.LockType == LockType.Exclusive)
            IncrementX(ref key);
        else
            IncrementS(ref key);
    }
    internal void Decrement(FixedLengthLockableKeyStruct<long> key) => Decrement(ref key);
    internal void Decrement(ref FixedLengthLockableKeyStruct<long> key)
    {
        if (key.LockType == LockType.Exclusive)
            DecrementX(ref key);
        else
            DecrementS(ref key);
    }

    internal void IncrementX(ref FixedLengthLockableKeyStruct<long> key) => AddX(ref key, 1);
    internal void DecrementX(ref FixedLengthLockableKeyStruct<long> key) => AddX(ref key, -1);
    internal void IncrementS(ref FixedLengthLockableKeyStruct<long> key) => AddS(ref key, 1);
    internal void DecrementS(ref FixedLengthLockableKeyStruct<long> key) => AddS(ref key, -1);

    private void AddX(ref FixedLengthLockableKeyStruct<long> key, int addend)
    {
        if (!buckets.TryGetValue(key.KeyHash, out (int x, int s) counts))
            counts = default;
        counts.x += addend;
        Assert.GreaterOrEqual(counts.x, 0);
        buckets[key.KeyHash] = counts;
    }

    private void AddS(ref FixedLengthLockableKeyStruct<long> key, int addend)
    {
        if (!buckets.TryGetValue(key.KeyHash, out (int x, int s) counts))
            counts = default;
        counts.s += addend;
        Assert.GreaterOrEqual(counts.s, 0);
        buckets[key.KeyHash] = counts;
    }

    internal bool GetLockCounts(ref FixedLengthLockableKeyStruct<long> key, out (int x, int s) counts)
    {
        if (!buckets.TryGetValue(key.KeyHash, out counts))
        {
            counts = default;
            return false;
        }
        return true;
    }

    internal (int x, int s) GetLockCounts()
    {
        int xx = 0;
        int ss = 0;
        foreach (KeyValuePair<long, (int x, int s)> kvp in buckets)
        {
            xx += kvp.Value.x;
            ss += kvp.Value.s;
        }
        return (xx, ss);
    }

    internal void AssertNoLocks()
    {
        foreach (KeyValuePair<long, (int x, int s)> kvp in buckets)
        {
            Assert.AreEqual(0, kvp.Value.x);
            Assert.AreEqual(0, kvp.Value.s);
        }
    }
}

[TestFixture]
internal class LockableUnsafeContextTests
{
    private const int numRecords = 1000;
    private const int useNewKey = 1010;
    private const int useExistingKey = 200;
    private const int valueMult = 1_000_000;
    private LockableUnsafeFunctions functions;
    private LockableUnsafeComparer comparer;

    private TsavoriteKV<long, long> store;
    private ClientSession<long, long, long, long, Empty, LockableUnsafeFunctions> session;
    private IDevice log;

    [SetUp]
    public void Setup() => Setup(forRecovery: false);

    public void Setup(bool forRecovery)
    {
        if (!forRecovery)
        {
            DeleteDirectory(MethodTestDir, wait: true);
        }
        log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false, recoverDevice: forRecovery);

        ReadCacheSettings readCacheSettings = default;
        CheckpointSettings checkpointSettings = default;
        foreach (object arg in TestContext.CurrentContext.Test.Arguments)
        {
            if (arg is ReadCopyDestination dest)
            {
                if (dest == ReadCopyDestination.ReadCache)
                    readCacheSettings = new() { PageSizeBits = 12, MemorySizeBits = 22 };
                break;
            }
            if (arg is CheckpointType chktType)
            {
                checkpointSettings = new CheckpointSettings { CheckpointDir = MethodTestDir };
                break;
            }
        }

        comparer = new LockableUnsafeComparer();
        functions = new LockableUnsafeFunctions();

        store = new TsavoriteKV<long, long>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22, ReadCacheSettings = readCacheSettings },
                                        checkpointSettings: checkpointSettings, comparer: comparer,
                                        concurrencyControlMode: ConcurrencyControlMode.LockTable);
        session = store.NewSession<long, long, Empty, LockableUnsafeFunctions>(functions);
    }

    [TearDown]
    public void TearDown() => TearDown(forRecovery: false);

    public void TearDown(bool forRecovery)
    {
        session?.Dispose();
        session = null;
        store?.Dispose();
        store = null;
        log?.Dispose();
        log = null;

        if (!forRecovery)
        {
            DeleteDirectory(MethodTestDir);
        }
    }

    private void Populate()
    {
        for (int key = 0; key < numRecords; key++)
            Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
    }

    private void AssertIsLocked(FixedLengthLockableKeyStruct<long> key, bool xlock, bool slock)
        => OverflowBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
    private void AssertIsLocked(ref FixedLengthLockableKeyStruct<long> key, bool xlock, bool slock)
        => OverflowBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);

    private void PrepareRecordLocation(FlushMode recordLocation) => PrepareRecordLocation(store, recordLocation);

    private static void PrepareRecordLocation(TsavoriteKV<long, long> store, FlushMode recordLocation)
    {
        if (recordLocation == FlushMode.ReadOnly)
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
        else if (recordLocation == FlushMode.OnDisk)
            store.Log.FlushAndEvict(wait: true);
    }

    private static void ClearCountsOnError(ClientSession<long, long, long, long, Empty, LockableUnsafeFunctions> luContext)
    {
        // If we already have an exception, clear these counts so "Run" will not report them spuriously.
        luContext.sharedLockCount = 0;
        luContext.exclusiveLockCount = 0;
    }

    private static void ClearCountsOnError<TFunctions>(ClientSession<long, long, long, long, Empty, TFunctions> luContext)
        where TFunctions : IFunctions<long, long, long, long, Empty>
    {
        // If we already have an exception, clear these counts so "Run" will not report them spuriously.
        luContext.sharedLockCount = 0;
        luContext.exclusiveLockCount = 0;
    }

    private void PopulateHei(ref HashEntryInfo hei) => OverflowBucketLockTableTests.PopulateHei(store, ref hei);

    private void AssertTotalLockCounts(long expectedX, long expectedS) => OverflowBucketLockTableTests.AssertTotalLockCounts(store, expectedX, expectedS);

    private unsafe void AssertTotalLockCounts(ref BucketLockTracker blt)
    {
        (int expectedX, int expectedS) = blt.GetLockCounts();
        AssertTotalLockCounts(expectedX, expectedS);

        foreach (KeyValuePair<long, (int x, int s)> kvp in blt.buckets)
        {
            HashBucket* hashBucket = store.LockTable.GetBucket(kvp.Key);
            Assert.AreEqual(kvp.Value.s, HashBucket.NumLatchedShared(hashBucket));
            Assert.AreEqual(kvp.Value.x == 1, HashBucket.IsLatchedExclusive(hashBucket));
        }
    }

    private void AssertNoLocks(ref BucketLockTracker blt)
    {
        blt.AssertNoLocks();
        AssertTotalLockCounts(0, 0);
    }

    internal void AssertBucketLockCount(ref FixedLengthLockableKeyStruct<long> key, long expectedX, long expectedS) => OverflowBucketLockTableTests.AssertBucketLockCount(store, ref key, expectedX, expectedS);

    internal enum LockOperationType { Lock, Unlock }

    internal static IEnumerable<int> EnumActionKeyIndices<TKey>(FixedLengthLockableKeyStruct<TKey>[] keys, LockOperationType lockOpType)
    {
        // "Action" means the keys that will actually be locked or unlocked.
        // See comments in LockableContext.DoInternalLockOp. Apps shouldn't need to do this; key sorting and enumeration
        // should be a black-box to them, so this code is just for test.
        if (lockOpType == LockOperationType.Lock)
        {
            for (int ii = 0; ii < keys.Length; ++ii)
            {
                if (ii == 0 || keys[ii].KeyHash != keys[ii - 1].KeyHash)
                    yield return ii;
            }
            yield break;
        }

        // LockOperationType.Unlock
        for (int ii = keys.Length - 1; ii >= 0; --ii)
        {
            if (ii == 0 || keys[ii].KeyHash != keys[ii - 1].KeyHash)
                yield return ii;
        }
    }

    [Test]
    [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
    public void ManualLockCollidingHashCodes([Values] UseSingleBucketComparer /* justToSignalSetup */ _)
    {
        // GetBucketIndex does a mask of lower bits.
        uint bucketIndex = 42;
        long genHashCode(uint uniquifier) => ((long)uniquifier << 30) | bucketIndex;

        LockableContext<long, long, long, long, Empty, LockableUnsafeFunctions> lContext = session.LockableContext;
        lContext.BeginLockable();

        FixedLengthLockableKeyStruct<long>[] keys = new[]
        {
            new FixedLengthLockableKeyStruct<long>(101L, genHashCode(1), LockType.Exclusive, lContext),
            new FixedLengthLockableKeyStruct<long>(102L, genHashCode(2), LockType.Exclusive, lContext),
            new FixedLengthLockableKeyStruct<long>(103L, genHashCode(3), LockType.Exclusive, lContext),
        };

        for (int ii = 0; ii < keys.Length; ++ii)
            Assert.AreEqual(bucketIndex, store.LockTable.GetBucketIndex(keys[ii].KeyHash), $"BucketIndex mismatch on key {ii}");

        lContext.Lock(keys);
        lContext.Unlock(keys);

        lContext.EndLockable();
    }

    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public async Task TestShiftHeadAddressLUC([Values] SyncMode syncMode)
    {
        long input = default;
        const int RandSeed = 10;
        const int RandRange = numRecords;
        const int NumRecs = 200;

        Random r = new(RandSeed);
        var sw = Stopwatch.StartNew();

        // Copied from UnsafeContextTests to test Async.
        LockableUnsafeContext<long, long, long, long, Empty, LockableUnsafeFunctions> luContext = session.LockableUnsafeContext;
        luContext.BeginUnsafe();
        luContext.BeginLockable();

        var keyVec = new FixedLengthLockableKeyStruct<long>[1];

        try
        {
            for (int c = 0; c < NumRecs; c++)
            {
                keyVec[0] = new(r.Next(RandRange), LockType.Exclusive, luContext);
                luContext.Lock(keyVec);
                AssertBucketLockCount(ref keyVec[0], 1, 0);

                long value = keyVec[0].Key + numRecords;
                if (syncMode == SyncMode.Sync)
                {
                    luContext.Upsert(ref keyVec[0].Key, ref value, Empty.Default, 0);
                }
                else
                {
                    luContext.EndUnsafe();
                    Status status = (await luContext.UpsertAsync(ref keyVec[0].Key, ref value)).Complete();
                    luContext.BeginUnsafe();
                    Assert.IsFalse(status.IsPending);
                }
                luContext.Unlock(keyVec);
                AssertBucketLockCount(ref keyVec[0], 0, 0);
            }

            AssertTotalLockCounts(0, 0);

            r = new Random(RandSeed);
            sw.Restart();

            for (int c = 0; c < NumRecs; c++)
            {
                keyVec[0] = new(r.Next(RandRange), LockType.Shared, luContext);
                long value = keyVec[0].Key + numRecords;
                long output = 0;

                luContext.Lock(keyVec);
                AssertBucketLockCount(ref keyVec[0], 0, 1);
                Status status;
                if (syncMode == SyncMode.Sync || (c % 1 == 0))  // in .Async mode, half the ops should be sync to test CompletePendingAsync
                {
                    status = luContext.Read(ref keyVec[0].Key, ref input, ref output, Empty.Default, 0);
                }
                else
                {
                    luContext.EndUnsafe();
                    (status, output) = (await luContext.ReadAsync(ref keyVec[0].Key, ref input)).Complete();
                    luContext.BeginUnsafe();
                }
                luContext.Unlock(keyVec);
                AssertBucketLockCount(ref keyVec[0], 0, 0);
                Assert.IsFalse(status.IsPending);
            }

            AssertTotalLockCounts(0, 0);

            if (syncMode == SyncMode.Sync)
            {
                luContext.CompletePending(true);
            }
            else
            {
                luContext.EndUnsafe();
                await luContext.CompletePendingAsync();
                luContext.BeginUnsafe();
            }

            // Shift head and retry - should not find in main memory now
            store.Log.FlushAndEvict(true);

            r = new Random(RandSeed);
            sw.Restart();

            // Since we do random selection with replacement, we may not lock all keys--so need to track which we do
            // Similarly, we need to track bucket counts.
            BucketLockTracker blt = new();
            FixedLengthLockableKeyStruct<long>[] lockKeys = Enumerable.Range(0, NumRecs).Select(ii => new FixedLengthLockableKeyStruct<long>(r.Next(RandRange), LockType.Shared, luContext)).ToArray();
            luContext.SortKeyHashes(lockKeys);
            luContext.Lock(lockKeys);

            int expectedS = 0;
            foreach (int idx in EnumActionKeyIndices(lockKeys, LockOperationType.Lock))
            {
                ++expectedS;
                long output = 0;
                blt.IncrementS(ref lockKeys[idx]);
                Status foundStatus = luContext.Read(ref lockKeys[idx].Key, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(foundStatus.IsPending);
            }

            // We did not lock all keys, only the "Action" ones - one lock per bucket, all shared in this test
            AssertTotalLockCounts(0, expectedS);

            CompletedOutputIterator<long, long, long, long, Empty> outputs;
            if (syncMode == SyncMode.Sync)
            {
                luContext.CompletePendingWithOutputs(out outputs, wait: true);
            }
            else
            {
                luContext.EndUnsafe();
                outputs = await luContext.CompletePendingWithOutputsAsync();
                luContext.BeginUnsafe();
            }

            foreach (int idx in EnumActionKeyIndices(lockKeys, LockOperationType.Unlock))
            {
                luContext.Unlock(lockKeys, idx, 1);
                blt.DecrementS(ref lockKeys[idx]);
            }

            blt.AssertNoLocks();
            AssertTotalLockCounts(0, 0);

            int count = 0;
            while (outputs.Next())
            {
                count++;
                Assert.AreEqual(outputs.Current.Key + numRecords, outputs.Current.Output);
            }
            outputs.Dispose();
            Assert.AreEqual(expectedS, count);
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void InMemorySimpleLockTxnTest([Values] ResultLockTarget resultLockTarget,
                                          [Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase,
                                          [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
    {
        Populate();
        PrepareRecordLocation(flushMode);

        // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
        bool useRMW = updateOp == UpdateOp.RMW;
        const int readKey24 = 24, readKey51 = 51;
        long resultKey = resultLockTarget == ResultLockTarget.LockTable ? numRecords + 1 : readKey24 + readKey51;
        long resultValue;
        long expectedResult = (readKey24 + readKey51) * valueMult;
        Status status;
        BucketLockTracker blt = new();

        LockableUnsafeContext<long, long, long, long, Empty, LockableUnsafeFunctions> luContext = session.LockableUnsafeContext;
        luContext.BeginUnsafe();
        luContext.BeginLockable();

        FixedLengthLockableKeyStruct<long>[] keys = new[]
        {
            new FixedLengthLockableKeyStruct<long>(readKey24, LockType.Shared, luContext),      // Source, shared
            new FixedLengthLockableKeyStruct<long>(readKey51, LockType.Shared, luContext),      // Source, shared
            new FixedLengthLockableKeyStruct<long>(resultKey, LockType.Exclusive, luContext),   // Destination, exclusive
        };
        luContext.SortKeyHashes(keys);

        try
        {
            luContext.Lock(keys);

            // Verify locks. Note that while we do not increment lock counts for multiple keys (each bucket gets a single lock per thread,
            // shared or exclusive), each key mapping to that bucket will report 'locked'.
            foreach (FixedLengthLockableKeyStruct<long> key in keys)
            {
                if (key.Key == resultKey)
                    AssertIsLocked(key, xlock: true, slock: false);
                else
                    AssertIsLocked(key, xlock: false, slock: true);
            }

            // Use blt because the counts are not 1:1 with keys if there are multiple keys in the same bucket
            foreach (int idx in EnumActionKeyIndices(keys, LockOperationType.Lock))
                blt.Increment(ref keys[idx]);
            AssertTotalLockCounts(ref blt);

            // Re-get source values, to verify (e.g. they may be in readcache now).
            // We just locked this above, but for FlushMode.OnDisk it will be in the LockTable and will still be PENDING.
            status = luContext.Read(readKey24, out long readValue24);
            if (flushMode == FlushMode.OnDisk)
            {
                if (status.IsPending)
                {
                    luContext.CompletePendingWithOutputs(out CompletedOutputIterator<long, long, long, long, Empty> completedOutputs, wait: true);
                    Assert.True(completedOutputs.Next());
                    readValue24 = completedOutputs.Current.Output;
                    Assert.AreEqual(24 * valueMult, readValue24);
                    Assert.False(completedOutputs.Next());
                    completedOutputs.Dispose();
                }
            }
            else
            {
                Assert.IsFalse(status.IsPending, status.ToString());
            }

            status = luContext.Read(readKey51, out long readValue51);
            if (flushMode == FlushMode.OnDisk)
            {
                if (status.IsPending)
                {
                    luContext.CompletePendingWithOutputs(out CompletedOutputIterator<long, long, long, long, Empty> completedOutputs, wait: true);
                    Assert.True(completedOutputs.Next());
                    readValue51 = completedOutputs.Current.Output;
                    Assert.AreEqual(51 * valueMult, readValue51);
                    Assert.False(completedOutputs.Next());
                    completedOutputs.Dispose();
                }
            }
            else
            {
                Assert.IsFalse(status.IsPending, status.ToString());
            }

            // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
            session.ctx.phase = phase;
            long dummyInOut = 0;
            status = useRMW
                ? luContext.RMW(ref resultKey, ref expectedResult, ref dummyInOut, out RecordMetadata recordMetadata)
                : luContext.Upsert(ref resultKey, ref dummyInOut, ref expectedResult, ref dummyInOut, out recordMetadata);
            if (flushMode == FlushMode.OnDisk)
            {
                if (status.IsPending)
                {
                    luContext.CompletePendingWithOutputs(out CompletedOutputIterator<long, long, long, long, Empty> completedOutputs, wait: true);
                    Assert.True(completedOutputs.Next());
                    resultValue = completedOutputs.Current.Output;
                    Assert.AreEqual(expectedResult, resultValue);
                    Assert.False(completedOutputs.Next());
                    completedOutputs.Dispose();
                }
            }
            else
            {
                Assert.IsFalse(status.IsPending, status.ToString());
            }

            // Reread the destination to verify
            status = luContext.Read(resultKey, out resultValue);
            Assert.IsFalse(status.IsPending, status.ToString());
            Assert.AreEqual(expectedResult, resultValue);

            luContext.Unlock(keys);

            foreach (int idx in EnumActionKeyIndices(keys, LockOperationType.Lock))
                blt.Decrement(ref keys[idx]);
            AssertNoLocks(ref blt);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }

        // Verify reading the destination from the full session.
        status = session.Read(resultKey, out resultValue);
        Assert.IsFalse(status.IsPending, status.ToString());
        Assert.AreEqual(expectedResult, resultValue);
        AssertTotalLockCounts(0, 0);
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void InMemoryLongLockTest([Values] ResultLockTarget resultLockTarget, [Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase,
                                     [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
    {
        Populate();
        PrepareRecordLocation(flushMode);

        bool initialDestWillBeLockTable = resultLockTarget == ResultLockTarget.LockTable || flushMode == FlushMode.OnDisk;
        const int readKey24 = 24, readKey51 = 51, valueMult2 = 10;
        long resultKey = initialDestWillBeLockTable ? numRecords + 1 : readKey24 + readKey51;
        long resultValue;
        int expectedResult = (readKey24 + readKey51) * valueMult * valueMult2;
        bool useRMW = updateOp == UpdateOp.RMW;
        Status status;
        BucketLockTracker blt = new();

        LockableUnsafeContext<long, long, long, long, Empty, LockableUnsafeFunctions> luContext = session.LockableUnsafeContext;
        luContext.BeginUnsafe();
        luContext.BeginLockable();

        FixedLengthLockableKeyStruct<long>[] keys = new[]
        {
            new FixedLengthLockableKeyStruct<long>(readKey24, LockType.Shared, luContext),      // Source, shared
            new FixedLengthLockableKeyStruct<long>(readKey51, LockType.Shared, luContext),      // Source, shared
            new FixedLengthLockableKeyStruct<long>(resultKey, LockType.Exclusive, luContext),   // Destination, exclusive
        };

        luContext.SortKeyHashes(keys);

        long[] buckets = keys.Select(key => store.LockTable.GetBucketIndex(key.KeyHash)).ToArray();

        try
        {
            luContext.Lock(keys);

            // Verify locks. Note that while we do not increment lock counts for multiple keys (each bucket gets a single lock per thread,
            // shared or exclusive), each key mapping to that bucket will report 'locked'.
            foreach (FixedLengthLockableKeyStruct<long> key in keys)
            {
                if (key.Key == resultKey)
                    AssertIsLocked(key, xlock: true, slock: false);
                else
                    AssertIsLocked(key, xlock: false, slock: true);
            }

            // Use blt because the counts are not 1:1 with keys if there are multiple keys in the same bucket
            foreach (int idx in EnumActionKeyIndices(keys, LockOperationType.Lock))
                blt.Increment(ref keys[idx]);
            AssertTotalLockCounts(ref blt);

            status = luContext.Read(readKey24, out long readValue24);
            if (flushMode == FlushMode.OnDisk)
            {
                Assert.IsTrue(status.IsPending, status.ToString());
                luContext.CompletePendingWithOutputs(out CompletedOutputIterator<long, long, long, long, Empty> completedOutputs, wait: true);
                (status, readValue24) = GetSinglePendingResult(completedOutputs, out RecordMetadata recordMetadata);
                Assert.IsTrue(status.Found, status.ToString());
            }
            else
                Assert.IsFalse(status.IsPending, status.ToString());
            Assert.AreEqual(readKey24 * valueMult, readValue24);

            // We just locked this above, but for FlushMode.OnDisk it will still be PENDING.
            status = luContext.Read(readKey51, out long readValue51);
            if (flushMode == FlushMode.OnDisk)
            {
                Assert.IsTrue(status.IsPending, status.ToString());
                luContext.CompletePendingWithOutputs(out CompletedOutputIterator<long, long, long, long, Empty> completedOutputs, wait: true);
                Assert.True(completedOutputs.Next());
                readValue51 = completedOutputs.Current.Output;
                Assert.False(completedOutputs.Next());
                completedOutputs.Dispose();
            }
            else
                Assert.IsFalse(status.IsPending, status.ToString());
            Assert.AreEqual(readKey51 * valueMult, readValue51);

            if (!initialDestWillBeLockTable)
            {
                status = luContext.Read(resultKey, out long initialResultValue);
                if (flushMode == FlushMode.OnDisk)
                {
                    Assert.IsTrue(status.IsPending, status.ToString());
                    luContext.CompletePendingWithOutputs(out CompletedOutputIterator<long, long, long, long, Empty> completedOutputs, wait: true);
                    (status, initialResultValue) = GetSinglePendingResult(completedOutputs, out RecordMetadata recordMetadata);
                    Assert.IsTrue(status.Found, status.ToString());
                }
                else
                    Assert.IsFalse(status.IsPending, status.ToString());
                Assert.AreEqual(resultKey * valueMult, initialResultValue);
            }

            // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
            session.ctx.phase = phase;
            status = useRMW
                ? luContext.RMW(resultKey, (readValue24 + readValue51) * valueMult2)
                : luContext.Upsert(resultKey, (readValue24 + readValue51) * valueMult2);
            Assert.IsFalse(status.IsPending, status.ToString());

            status = luContext.Read(resultKey, out resultValue);
            Assert.IsFalse(status.IsPending, status.ToString());
            Assert.AreEqual(expectedResult, resultValue);

            luContext.Unlock(keys);

            foreach (int idx in EnumActionKeyIndices(keys, LockOperationType.Lock))
                blt.Decrement(ref keys[idx]);
            AssertNoLocks(ref blt);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }

        // Verify from the full session.
        status = session.Read(resultKey, out resultValue);
        Assert.IsFalse(status.IsPending, status.ToString());
        Assert.AreEqual(expectedResult, resultValue);
        AssertTotalLockCounts(0, 0);
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
#pragma warning disable IDE0060 // Remove unused parameter: readCopyDestination is used by Setup
    public void InMemoryDeleteTest([Values] ResultLockTarget resultLockTarget, [Values] ReadCopyDestination readCopyDestination,
                                   [Values(FlushMode.NoFlush, FlushMode.ReadOnly)] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
#pragma warning restore IDE0060 // Remove unused parameter
    {
        // Phase.INTERMEDIATE is to test the non-Phase.REST blocks
        Populate();
        PrepareRecordLocation(flushMode);

        BucketLockTracker blt = new();

        // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
        long resultKey = resultLockTarget == ResultLockTarget.LockTable ? numRecords + 1 : 75;
        Status status;

        LockableUnsafeContext<long, long, long, long, Empty, LockableUnsafeFunctions> luContext = session.LockableUnsafeContext;
        luContext.BeginUnsafe();
        luContext.BeginLockable();

        FixedLengthLockableKeyStruct<long>[] keyVec = new[] { new FixedLengthLockableKeyStruct<long>(resultKey, LockType.Exclusive, luContext) };

        try
        {
            // Lock destination value.
            luContext.Lock(keyVec);
            AssertIsLocked(ref keyVec[0], xlock: true, slock: false);

            blt.Increment(ref keyVec[0]);
            AssertTotalLockCounts(ref blt);

            // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
            session.ctx.phase = phase;
            status = luContext.Delete(ref resultKey);
            Assert.IsFalse(status.IsPending, status.ToString());

            // Reread the destination to verify
            status = luContext.Read(resultKey, out long _);
            Assert.IsFalse(status.Found, status.ToString());

            luContext.Unlock(keyVec);
            blt.Decrement(ref keyVec[0]);

            AssertNoLocks(ref blt);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }

        // Verify reading the destination from the full session.
        status = session.Read(resultKey, out long _);
        Assert.IsFalse(status.Found, status.ToString());
        AssertTotalLockCounts(0, 0);
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void StressManualLocks([Values(1, 8)] int numLockThreads, [Values(0, 1, 8)] int numOpThreads)
    {
        Populate();

        // Lock in ordered sequence (avoiding deadlocks)
        const int baseKey = 42;
        const int numKeys = 20;
        const int numIncrement = 5;
        const int numIterations = 1000;

        IEnumerable<int> enumKeys(Random rng)
        {
            for (int key = baseKey + rng.Next(numIncrement); key < baseKey + numKeys; key += rng.Next(1, numIncrement))
                yield return key;
        }

        void runManualLockThread(int tid)
        {
            BucketLockTracker blt = new();

            Random rng = new(tid + 101);

            using ClientSession<long, long, long, long, Empty, LockableUnsafeFunctions> localSession = store.NewSession<long, long, Empty, LockableUnsafeFunctions>(new LockableUnsafeFunctions());
            LockableUnsafeContext<long, long, long, long, Empty, LockableUnsafeFunctions> luContext = localSession.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            IEnumerable<FixedLengthLockableKeyStruct<long>> enumKeysToLock()
            {
                foreach (int key in enumKeys(rng))
                {
                    LockType lockType = rng.Next(100) < 60 ? LockType.Shared : LockType.Exclusive;
                    yield return new(key, lockType, luContext);
                }
            }

            for (int iteration = 0; iteration < numIterations; ++iteration)
            {
                FixedLengthLockableKeyStruct<long>[] keys = enumKeysToLock().ToArray();
                FixedLengthLockableKeyStruct<long>.Sort(keys, luContext);
                luContext.Lock(keys);
                luContext.Unlock(keys);
            }

            luContext.EndLockable();
            luContext.EndUnsafe();
        }

        void runLTransientLockOpThread(int tid)
        {
            Random rng = new(tid + 101);

            using ClientSession<long, long, long, long, Empty, LockableUnsafeFunctions> localSession = store.NewSession<long, long, Empty, LockableUnsafeFunctions>(new LockableUnsafeFunctions());
            BasicContext<long, long, long, long, Empty, LockableUnsafeFunctions> basicContext = localSession.BasicContext;

            for (int iteration = 0; iteration < numIterations; ++iteration)
            {
                foreach (int key in enumKeys(rng))
                {
                    int rand = rng.Next(100);
                    if (rand < 33)
                        basicContext.Read(key);
                    else if (rand < 66)
                        basicContext.Upsert(key, key * valueMult);
                    else
                        basicContext.RMW(key, key * valueMult);
                }
            }
        }

        // Run a mix of luContext and normal ClientSession operations
        int numThreads = numLockThreads + numOpThreads;
        Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
        for (int t = 0; t < numThreads; t++)
        {
            int tid = t;
            if (t <= numLockThreads)
                tasks[t] = Task.Factory.StartNew(() => runManualLockThread(tid));
            else
                tasks[t] = Task.Factory.StartNew(() => runLTransientLockOpThread(tid));
        }
        Task.WaitAll(tasks);

        AssertTotalLockCounts(0, 0);
    }

    private FixedLengthLockableKeyStruct<long> AddLockTableEntry<TFunctions>(LockableUnsafeContext<long, long, long, long, Empty, TFunctions> luContext, long key)
        where TFunctions : IFunctions<long, long, long, long, Empty>
    {
        FixedLengthLockableKeyStruct<long>[] keyVec = new[] { new FixedLengthLockableKeyStruct<long>(key, LockType.Exclusive, luContext) };
        luContext.Lock(keyVec);

        HashEntryInfo hei = new(comparer.GetHashCode64(ref key));
        PopulateHei(ref hei);

        LockState lockState = store.LockTable.GetLockState(ref key, ref hei);

        Assert.IsTrue(lockState.IsFound);
        Assert.IsTrue(lockState.IsLockedExclusive);
        return keyVec[0];
    }

    private void VerifyAndUnlockSplicedInKey<TFunctions>(LockableUnsafeContext<long, long, long, long, Empty, TFunctions> luContext, long expectedKey)
        where TFunctions : IFunctions<long, long, long, long, Empty>
    {
        // Scan to the end of the readcache chain and verify we inserted the value.
        (long _, long pa) = ChainTests.SkipReadCacheChain(store, expectedKey);
        long storedKey = store.hlog.GetKey(pa);
        Assert.AreEqual(expectedKey, storedKey);

        FixedLengthLockableKeyStruct<long>[] keyVec = new[] { new FixedLengthLockableKeyStruct<long>(expectedKey, LockType.Exclusive, luContext) };
        luContext.Unlock(keyVec);
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void VerifyLocksAfterReadAndCTTTest()
    {
        Populate();
        store.Log.FlushAndEvict(wait: true);

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;
        long input = 0, output = 0, key = 24;
        ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };
        BucketLockTracker blt = new();

        luContext.BeginUnsafe();
        luContext.BeginLockable();
        try
        {
            FixedLengthLockableKeyStruct<long> keyStruct = AddLockTableEntry(luContext, key);
            blt.Increment(ref keyStruct);
            AssertTotalLockCounts(ref blt);

            Status status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending, status.ToString());
            luContext.CompletePending(wait: true);

            VerifyAndUnlockSplicedInKey(luContext, key);
            blt.Decrement(ref keyStruct);
            AssertNoLocks(ref blt);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void VerifyCountsAfterFlushAndEvict()
    {
        Populate();

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;
        BucketLockTracker blt = new();
        long key = 24;

        luContext.BeginUnsafe();
        luContext.BeginLockable();
        try
        {
            FixedLengthLockableKeyStruct<long>[] keyVec = new[] { new FixedLengthLockableKeyStruct<long>(key, LockType.Exclusive, luContext) };
            luContext.Lock(keyVec);
            blt.Increment(ref keyVec[0]);
            AssertTotalLockCounts(ref blt);

            store.Log.FlushAndEvict(wait: true);
            AssertTotalLockCounts(1, 0);

            luContext.Unlock(keyVec);
            blt.Decrement(ref keyVec[0]);

            blt.AssertNoLocks();
            AssertNoLocks(ref blt);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    private void PopulateAndEvict(bool immutable = false)
    {
        Populate();

        if (immutable)
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
        else
            store.Log.FlushAndEvict(true);
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void VerifyCountAfterUpsertToTailTest([Values] ChainTests.RecordRegion recordRegion)
    {
        PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;
        BucketLockTracker blt = new();
        luContext.BeginUnsafe();
        luContext.BeginLockable();

        FixedLengthLockableKeyStruct<long> keyStruct = default;
        try
        {
            if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
                keyStruct = AddLockTableEntry(luContext, useExistingKey);
            else
                keyStruct = AddLockTableEntry(luContext, useNewKey);
            blt.Increment(ref keyStruct);
            Status status = luContext.Upsert(keyStruct.Key, keyStruct.Key * valueMult);
            Assert.IsTrue(status.Record.Created, status.ToString());

            VerifyAndUnlockSplicedInKey(luContext, keyStruct.Key);
            blt.Decrement(ref keyStruct);
            AssertNoLocks(ref blt);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void VerifyCountAfterRMWToTailTest([Values] ChainTests.RecordRegion recordRegion)
    {
        PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;
        BucketLockTracker blt = new();
        luContext.BeginUnsafe();
        luContext.BeginLockable();

        FixedLengthLockableKeyStruct<long> keyStruct = default;
        try
        {
            if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
            {
                keyStruct = AddLockTableEntry(luContext, useExistingKey);
                Status status = luContext.RMW(keyStruct.Key, keyStruct.Key * valueMult);
                Assert.IsTrue(recordRegion == ChainTests.RecordRegion.OnDisk ? status.IsPending : status.Found);
                luContext.CompletePending(wait: true);
            }
            else
            {
                keyStruct = AddLockTableEntry(luContext, useNewKey);
                Status status = luContext.RMW(keyStruct.Key, keyStruct.Key * valueMult);
                Assert.IsFalse(status.Found, status.ToString());
            }
            blt.Increment(ref keyStruct);

            VerifyAndUnlockSplicedInKey(luContext, keyStruct.Key);
            blt.Decrement(ref keyStruct);
            AssertNoLocks(ref blt);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void VerifyCountAfterDeleteToTailTest([Values] ChainTests.RecordRegion recordRegion)
    {
        PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;
        BucketLockTracker blt = new();
        luContext.BeginUnsafe();
        luContext.BeginLockable();

        FixedLengthLockableKeyStruct<long> keyStruct = default;
        try
        {
            if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
            {
                keyStruct = AddLockTableEntry(luContext, useExistingKey);
                blt.Increment(ref keyStruct);
                Status status = luContext.Delete(keyStruct.Key);

                // Delete does not search outside mutable region so the key will not be found
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }
            else
            {
                keyStruct = AddLockTableEntry(luContext, useNewKey);
                blt.Increment(ref keyStruct);
                Status status = luContext.Delete(keyStruct.Key);
                Assert.IsFalse(status.Found, status.ToString());
            }

            VerifyAndUnlockSplicedInKey(luContext, keyStruct.Key);
            blt.Decrement(ref keyStruct);
            AssertNoLocks(ref blt);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void LockAndUnlockInLockTableOnlyTest()
    {
        // For this, just don't load anything, and it will happen in lock table.
        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;
        BucketLockTracker blt = new();

        FixedLengthLockableKeyStruct<long> createKey(long key) => new(key, (key & 1) == 0 ? LockType.Exclusive : LockType.Shared, luContext);

        var rng = new Random(101);
        FixedLengthLockableKeyStruct<long>[] keyVec = Enumerable.Range(0, numRecords).Select(ii => createKey(rng.Next(numRecords))).ToArray();

        luContext.BeginUnsafe();
        luContext.BeginLockable();
        try
        {
            store.LockTable.SortKeyHashes(keyVec);
            luContext.Lock(keyVec);
            foreach (int idx in EnumActionKeyIndices(keyVec, LockOperationType.Lock))
                blt.Increment(ref keyVec[idx]);
            AssertTotalLockCounts(ref blt);

            foreach (int idx in EnumActionKeyIndices(keyVec, LockOperationType.Lock))
            {
                ref FixedLengthLockableKeyStruct<long> key = ref keyVec[idx];
                HashEntryInfo hei = new(key.KeyHash);
                PopulateHei(ref hei);
                LockState lockState = store.LockTable.GetLockState(ref key.Key, ref hei);
                Assert.IsTrue(lockState.IsFound);
                Assert.AreEqual(key.LockType == LockType.Exclusive, lockState.IsLockedExclusive);
                if (key.LockType == LockType.Shared)
                    Assert.IsTrue(lockState.IsLocked);    // Could be either shared or exclusive; we only lock the bucket once per Lock() call

                luContext.Unlock(keyVec, idx, 1);
                blt.Decrement(ref key);
            }

            blt.AssertNoLocks();
            AssertTotalLockCounts(0, 0);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void VerifyCountAfterReadOnlyToUpdateRecordTest([Values] UpdateOp updateOp)
    {
        Populate();
        store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

        static long getValue(long key) => key + valueMult;

        LockableUnsafeContext<long, long, long, long, Empty, LockableUnsafeFunctions> luContext = session.LockableUnsafeContext;
        luContext.BeginUnsafe();
        luContext.BeginLockable();

        FixedLengthLockableKeyStruct<long>[] keyVec = new[] { new FixedLengthLockableKeyStruct<long>(42, LockType.Exclusive, luContext) };

        try
        {
            luContext.Lock(keyVec);

            Status status = updateOp switch
            {
                UpdateOp.Upsert => luContext.Upsert(keyVec[0].Key, getValue(keyVec[0].Key)),
                UpdateOp.RMW => luContext.RMW(keyVec[0].Key, getValue(keyVec[0].Key)),
                UpdateOp.Delete => luContext.Delete(keyVec[0].Key),
                _ => new(StatusCode.Error)
            };
            Assert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
            if (updateOp == UpdateOp.RMW)
                Assert.IsTrue(status.Record.CopyUpdated, status.ToString());
            else
                Assert.IsTrue(status.Record.Created, status.ToString());

            OverflowBucketLockTableTests.AssertLockCounts(store, keyVec[0].Key, true, 0);

            luContext.Unlock(keyVec);
            OverflowBucketLockTableTests.AssertLockCounts(store, keyVec[0].Key, false, 0);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    public void LockNewRecordThenUpdateAndUnlockTest([Values] UpdateOp updateOp)
    {
        const int numNewRecords = 100;

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;

        int getValue(int key) => key + valueMult;

        // If we are testing Delete, then we need to have the records ON-DISK first; Delete is a no-op for unfound records.
        if (updateOp == UpdateOp.Delete)
        {
            for (int key = numRecords; key < numRecords + numNewRecords; ++key)
                Assert.IsFalse(this.session.Upsert(key, key * valueMult).IsPending);
            store.Log.FlushAndEvict(wait: true);
        }

        // Now populate the main area of the log.
        Populate();
        BucketLockTracker blt = new();

        luContext.BeginUnsafe();
        luContext.BeginLockable();

        var keyVec = new FixedLengthLockableKeyStruct<long>[1];

        try
        {
            // We don't sleep in this test
            comparer.maxSleepMs = 0;

            for (int key = numRecords; key < numRecords + numNewRecords; ++key)
            {
                keyVec[0] = new(key, LockType.Exclusive, luContext);
                luContext.Lock(keyVec);
                for (int iter = 0; iter < 2; ++iter)
                {
                    OverflowBucketLockTableTests.AssertLockCounts(store, key, true, 0);
                    updater(key, iter);
                }
                luContext.Unlock(keyVec);
                OverflowBucketLockTableTests.AssertLockCounts(store, key, false, 0);
            }
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }

        void updater(int key, int iter)
        {
            try
            {
                Status status;
                switch (updateOp)
                {
                    case UpdateOp.Upsert:
                        status = luContext.Upsert(key, getValue(key));
                        if (iter == 0)
                            Assert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                        else
                            Assert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                        break;
                    case UpdateOp.RMW:
                        status = luContext.RMW(key, getValue(key));
                        if (iter == 0)
                            Assert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                        else
                            Assert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                        break;
                    case UpdateOp.Delete:
                        status = luContext.Delete(key);
                        Assert.IsTrue(status.NotFound, status.ToString());
                        if (iter == 0)
                            Assert.IsTrue(status.Record.Created, status.ToString());
                        break;
                    default:
                        Assert.Fail($"Unexpected updateOp {updateOp}");
                        return;
                };
                Assert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    //[Repeat(100)]
    public void LockNewRecordThenUnlockThenUpdateTest([Values] UpdateOp updateOp)
    {
        if (TestContext.CurrentContext.CurrentRepeatCount > 0)
            Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

        const int numNewRecords = 50;

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> lockSession = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> lockLuContext = lockSession.LockableUnsafeContext;

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> updateSession = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        BasicContext<long, long, long, long, Empty, SimpleFunctions<long, long>> basicContext = updateSession.BasicContext;

        int getValue(int key) => key + valueMult;

        // If we are testing Delete, then we need to have the records ON-DISK first; Delete is a no-op for unfound records.
        if (updateOp == UpdateOp.Delete)
        {
            for (int key = numRecords; key < numRecords + numNewRecords; ++key)
                Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
            store.Log.FlushAndEvict(wait: true);
        }

        // Now populate the main area of the log.
        Populate();

        lockLuContext.BeginUnsafe();
        lockLuContext.BeginLockable();

        // These are for debugging
        int[] lastLockerKeys = new int[6], lastUpdaterKeys = new int[3];

        // Randomize the start and lock-hold wait times
        int maxSleepMs = 10;
        Random lockRng = new(101), updateRng = new(107);

        var lockKeyVec = new FixedLengthLockableKeyStruct<long>[1];

        try
        {
            for (int key = numRecords; key < numRecords + numNewRecords; ++key)
            {
                for (int iter = 0; iter < 2; ++iter)
                {
                    // Use Task instead of Thread because this propagates exceptions (such as Assert.* failures) back to this thread.
                    // BasicContext's transient lock will wait for the lock/unlock combo to complete, or the lock/unlock will wait for basicContext to finish if it wins.
                    Task.WaitAll(Task.Run(() => locker(key)), Task.Run(() => updater(key, iter)));
                }

                AssertBucketLockCount(ref lockKeyVec[0], 0, 0);
            }
        }
        catch (Exception)
        {
            ClearCountsOnError(lockSession);
            throw;
        }
        finally
        {
            lockLuContext.EndLockable();
            lockLuContext.EndUnsafe();
        }

        void locker(int key)
        {
            lockKeyVec[0] = new(key, LockType.Exclusive, lockLuContext);
            try
            {
                // Begin/EndLockable are called outside this function; we could not EndLockable in here as the lock lifetime is beyond that.
                // (BeginLockable's scope is the session; BeginUnsafe's scope is the thread. The session is still "mono-threaded" here because
                // only one thread at a time is making calls on it.)
                lastLockerKeys[0] = key;
                lockLuContext.BeginUnsafe();
                lastLockerKeys[1] = key;
                Thread.Sleep(lockRng.Next(maxSleepMs));
                lastLockerKeys[2] = key;
                lockLuContext.Lock(lockKeyVec);
                lastLockerKeys[3] = key;
                Thread.Sleep(lockRng.Next(maxSleepMs));
                lastLockerKeys[4] = key;
                lockLuContext.Unlock(lockKeyVec);
                lastLockerKeys[5] = key;
            }
            catch (Exception)
            {
                ClearCountsOnError(lockSession);
                throw;
            }
            finally
            {
                lockLuContext.EndUnsafe();
            }
        }

        void updater(int key, int iter)
        {
            try
            {
                lastUpdaterKeys[0] = key;
                Thread.Sleep(updateRng.Next(maxSleepMs));
                lastUpdaterKeys[1] = key;
                Status status;
                switch (updateOp)
                {
                    case UpdateOp.Upsert:
                        status = basicContext.Upsert(key, getValue(key));
                        if (iter == 0)
                            Assert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                        else
                            Assert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                        break;
                    case UpdateOp.RMW:
                        status = basicContext.RMW(key, getValue(key));
                        if (iter == 0)
                            Assert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                        else
                            Assert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                        break;
                    case UpdateOp.Delete:
                        status = basicContext.Delete(key);
                        Assert.IsTrue(status.NotFound, status.ToString());
                        if (iter == 0)
                            Assert.IsTrue(status.Record.Created, status.ToString());
                        break;
                    default:
                        Assert.Fail($"Unexpected updateOp {updateOp}");
                        return;
                };
                Assert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
                lastUpdaterKeys[2] = key;
            }
            catch (Exception)
            {
                ClearCountsOnError(lockSession);
                throw;
            }
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void MultiSharedLockTest()
    {
        Populate();

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;

        const int key = 42;
        int maxLocks = 63;

        luContext.BeginUnsafe();
        luContext.BeginLockable();

        var keyVec = new FixedLengthLockableKeyStruct<long>[1];

        try
        {
            for (int ii = 0; ii < maxLocks; ++ii)
            {
                keyVec[0] = new(key, LockType.Shared, luContext);
                luContext.Lock(keyVec);
                OverflowBucketLockTableTests.AssertLockCounts(store, key, false, ii + 1);
            }

            for (int ii = 0; ii < maxLocks; ++ii)
            {
                keyVec[0] = new(key, LockType.Shared, luContext);
                luContext.Unlock(keyVec);
                OverflowBucketLockTableTests.AssertLockCounts(store, key, false, maxLocks - ii - 1);
            }
            OverflowBucketLockTableTests.AssertLockCounts(store, key, false, 0);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void TryLockTimeSpanLimitTest()
    {
        Populate();

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;

        luContext.BeginUnsafe();
        luContext.BeginLockable();

        var keyVec = new FixedLengthLockableKeyStruct<long>[]
        {
            new(42, LockType.Exclusive, luContext),
            new(43, LockType.Exclusive, luContext),
            new(44, LockType.Exclusive, luContext)
        };

        // First ensure things work with no blocking locks.
        Assert.IsTrue(luContext.TryLock(keyVec));
        luContext.Unlock(keyVec);

        var blockingVec = new FixedLengthLockableKeyStruct<long>[1];

        try
        {
            for (int blockingIdx = 0; blockingIdx < keyVec.Length; ++blockingIdx)
            {
                // This key blocks the lock. Test all positions in keyVec to ensure rollback of locks on failure.
                blockingVec[0] = keyVec[blockingIdx];
                luContext.Lock(blockingVec);

                // Now try the lock, and verify there are no locks left after (any taken must be rolled back on failure).
                Assert.IsFalse(luContext.TryLock(keyVec, TimeSpan.FromMilliseconds(20)));
                foreach (FixedLengthLockableKeyStruct<long> k in keyVec)
                {
                    if (k.Key != blockingVec[0].Key)
                        OverflowBucketLockTableTests.AssertLockCounts(store, k.Key, false, 0);
                }

                luContext.Unlock(blockingVec);
            }
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void TryLockCancellationTest()
    {
        Populate();

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;

        luContext.BeginUnsafe();
        luContext.BeginLockable();

        var keyVec = new FixedLengthLockableKeyStruct<long>[]
        {
            new(42, LockType.Exclusive, luContext),
            new(43, LockType.Exclusive, luContext),
            new(44, LockType.Exclusive, luContext)
        };

        // First ensure things work with no blocking locks.
        Assert.IsTrue(luContext.TryLock(keyVec));
        luContext.Unlock(keyVec);

        var blockingVec = new FixedLengthLockableKeyStruct<long>[1];

        try
        {
            for (int blockingIdx = 0; blockingIdx < keyVec.Length; ++blockingIdx)
            {
                // This key blocks the lock. Test all positions in keyVec to ensure rollback of locks on failure.
                blockingVec[0] = keyVec[blockingIdx];
                luContext.Lock(blockingVec);

                using var cts = new CancellationTokenSource(20);

                // Now try the lock, and verify there are no locks left after (any taken must be rolled back on failure).
                Assert.IsFalse(luContext.TryLock(keyVec, cts.Token));
                foreach (FixedLengthLockableKeyStruct<long> k in keyVec)
                {
                    if (k.Key != blockingVec[0].Key)
                        OverflowBucketLockTableTests.AssertLockCounts(store, k.Key, false, 0);
                }

                luContext.Unlock(blockingVec);
            }
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void TryPromoteLockTimeSpanLimitTest()
    {
        Populate();

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;

        luContext.BeginUnsafe();
        luContext.BeginLockable();

        int key = 42;

        var exclusiveVec = new FixedLengthLockableKeyStruct<long>[] { new(key, LockType.Exclusive, luContext) };
        var sharedVec = new FixedLengthLockableKeyStruct<long>[] { new(key, LockType.Shared, luContext) };

        try
        {
            // Lock twice so it is blocked by the second reader
            Assert.IsTrue(luContext.TryLock(sharedVec));
            Assert.IsTrue(luContext.TryLock(sharedVec));

            Assert.IsFalse(luContext.TryPromoteLock(exclusiveVec[0], TimeSpan.FromMilliseconds(20)));

            // Unlock one of the readers and verify successful promotion
            luContext.Unlock(sharedVec);
            Assert.IsTrue(luContext.TryPromoteLock(exclusiveVec[0]));
            luContext.Unlock(exclusiveVec);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }

    [Test]
    [Category(LockableUnsafeContextTestCategory)]
    [Category(SmokeTestCategory)]
    public void TryPromoteLockCancellationTest()
    {
        Populate();

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        LockableUnsafeContext<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext = session.LockableUnsafeContext;

        luContext.BeginUnsafe();
        luContext.BeginLockable();

        int key = 42;

        var exclusiveVec = new FixedLengthLockableKeyStruct<long>[] { new(key, LockType.Exclusive, luContext) };
        var sharedVec = new FixedLengthLockableKeyStruct<long>[] { new(key, LockType.Shared, luContext) };

        try
        {
            // Lock twice so it is blocked by the second reader
            Assert.IsTrue(luContext.TryLock(sharedVec));
            Assert.IsTrue(luContext.TryLock(sharedVec));

            using var cts = new CancellationTokenSource(20);
            Assert.IsFalse(luContext.TryPromoteLock(exclusiveVec[0], cts.Token));

            // Unlock one of the readers and verify successful promotion
            luContext.Unlock(sharedVec);
            Assert.IsTrue(luContext.TryPromoteLock(exclusiveVec[0]));
            luContext.Unlock(exclusiveVec);
        }
        catch (Exception)
        {
            ClearCountsOnError(session);
            throw;
        }
        finally
        {
            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }
}