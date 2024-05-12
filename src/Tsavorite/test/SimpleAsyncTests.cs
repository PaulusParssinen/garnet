﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.Tests.Recovery.Sumstore;

namespace Tsavorite.Tests.async;

[TestFixture]
public class SimpleAsyncTests
{
    private IDevice log;
    private TsavoriteKV<long, long> store;
    private const int numOps = 5000;
    private AdId[] inputArray;

    [SetUp]
    public void Setup()
    {
        inputArray = new AdId[numOps];
        for (int i = 0; i < numOps; i++)
        {
            inputArray[i].adId = i;
        }

        TestUtils.RecreateDirectory(TestUtils.MethodTestDir);
        log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "Async.log"), deleteOnClose: true);
        store = new TsavoriteKV<long, long>
            (1L << 10,
            logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 15 },
            checkpointSettings: new CheckpointSettings { CheckpointDir = TestUtils.MethodTestDir }
            );
    }

    [TearDown]
    public void TearDown()
    {
        store?.Dispose();
        store = null;
        log?.Dispose();
        log = null;
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
    }

    // Test that does .ReadAsync with minimum parameters (ref key)
    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public async Task ReadAsyncMinParamTest()
    {
        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        for (long key = 0; key < numOps; key++)
        {
            TsavoriteKV<long, long>.UpsertAsyncResult<long, long, Empty> r = await s1.UpsertAsync(ref key, ref key);
            while (r.Status.IsPending)
                r = await r.CompleteAsync(); // test async version of Upsert completion
        }

        for (long key = 0; key < numOps; key++)
        {
            (Status status, long output) = (await s1.ReadAsync(ref key)).Complete();
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key, output);
        }
    }

    // Test that does .ReadAsync with minimum parameters but no default (ref key, userContext, serialNo, token)
    [Test]
    [Category("TsavoriteKV")]
    public async Task ReadAsyncMinParamTestNoDefaultTest()
    {
        CancellationToken cancellationToken = default;

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        for (long key = 0; key < numOps; key++)
        {
            TsavoriteKV<long, long>.UpsertAsyncResult<long, long, Empty> r = await s1.UpsertAsync(ref key, ref key);
            r.Complete(); // test sync version of Upsert completion
        }

        for (long key = 0; key < numOps; key++)
        {
            (Status status, long output) = (await s1.ReadAsync(ref key, Empty.Default, 99, cancellationToken)).Complete();
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key, output);
        }
    }

    // Test that does .ReadAsync no ref key (key)
    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public async Task ReadAsyncNoRefKeyTest()
    {
        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        for (long key = 0; key < numOps; key++)
        {
            TsavoriteKV<long, long>.UpsertAsyncResult<long, long, Empty> r = await s1.UpsertAsync(ref key, ref key);
            r.Complete(); // test sync version of Upsert completion
        }

        for (long key = 0; key < numOps; key++)
        {
            (Status status, long output) = (await s1.ReadAsync(key, Empty.Default, 99)).Complete();
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key, output);
        }
    }

    // Test that does .ReadAsync ref key and ref input (ref key, ref input)
    [Test]
    [Category("TsavoriteKV")]
    public async Task ReadAsyncRefKeyRefInputTest()
    {
        Status status;
        long key = default, input = default, output = default;

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>((a, b) => a + b));
        for (key = 0; key < numOps; key++)
        {
            (await s1.RMWAsync(ref key, ref key)).Complete();
        }

        for (key = 0; key < numOps; key++)
        {
            (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key, output);
        }

        key = 0;
        input = 35;
        ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>> t1 = s1.RMWAsync(ref key, ref input);
        ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>> t2 = s1.RMWAsync(ref key, ref input);

        (await t1).Complete();
        (await t2).Complete(); // should trigger RMW re-do

        (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
        Assert.IsTrue(status.Found);
        Assert.AreEqual(key + input + input, output);
    }


    // Test that does .ReadAsync no ref key and no ref input (key, input)
    [Test]
    [Category("TsavoriteKV")]
    public async Task ReadAsyncNoRefKeyNoRefInputTest()
    {
        Status status;
        long key = default, input = default, output = default;

        using ClientSession<long, long, long, long, Empty, RMWSimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, RMWSimpleFunctions<long, long>>(new RMWSimpleFunctions<long, long>((a, b) => a + b));
        for (key = 0; key < numOps; key++)
        {
            (status, output) = (await s1.RMWAsync(ref key, ref key, Empty.Default)).Complete();
            Assert.IsFalse(status.IsPending);
            Assert.AreEqual(key, output);
        }

        for (key = 0; key < numOps; key++)
        {
            (status, output) = (await s1.ReadAsync(key, output)).Complete();
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key, output);
        }

        key = 0;
        input = 9912;
        ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>> t1 = s1.RMWAsync(ref key, ref input);
        ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>> t2 = s1.RMWAsync(ref key, ref input);

        (await t1).Complete();
        (await t2).Complete(); // should trigger RMW re-do

        (status, output) = (await s1.ReadAsync(key, output, Empty.Default, 129)).Complete();
        Assert.IsTrue(status.Found);
        Assert.AreEqual(key + input + input, output);
    }

    // Test that does .UpsertAsync, .ReadAsync, .DeleteAsync, .ReadAsync with minimum parameters passed by reference (ref key)
    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public async Task UpsertReadDeleteReadAsyncMinParamByRefTest()
    {
        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        for (long key = 0; key < numOps; key++)
        {
            TsavoriteKV<long, long>.UpsertAsyncResult<long, long, Empty> r = await s1.UpsertAsync(ref key, ref key);
            while (r.Status.IsPending)
                r = await r.CompleteAsync(); // test async version of Upsert completion
        }

        Assert.Greater(numOps, 100);

        for (long key = 0; key < numOps; key++)
        {
            (Status status, long output) = (await s1.ReadAsync(ref key)).Complete();
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key, output);
        }

        {   // Scope for variables
            long deleteKey = 99;
            TsavoriteKV<long, long>.DeleteAsyncResult<long, long, Empty> r = await s1.DeleteAsync(ref deleteKey);
            while (r.Status.IsPending)
                r = await r.CompleteAsync(); // test async version of Delete completion

            (Status status, long _) = (await s1.ReadAsync(ref deleteKey)).Complete();
            Assert.IsFalse(status.Found);
        }
    }

    // Test that does .UpsertAsync, .ReadAsync, .DeleteAsync, .ReadAsync with minimum parameters passed by value (key)
    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public async Task UpsertReadDeleteReadAsyncMinParamByValueTest()
    {
        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
        for (long key = 0; key < numOps; key++)
        {
            Status status = (await s1.UpsertAsync(key, key)).Complete();   // test sync version of Upsert completion
            Assert.IsFalse(status.IsPending);
        }

        Assert.Greater(numOps, 100);

        for (long key = 0; key < numOps; key++)
        {
            (Status status, long output) = (await s1.ReadAsync(key)).Complete();
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key, output);
        }

        {   // Scope for variables
            long deleteKey = 99;
            Status status = (await s1.DeleteAsync(deleteKey)).Complete(); // test sync version of Delete completion
            Assert.IsFalse(status.IsPending);

            (status, _) = (await s1.ReadAsync(deleteKey)).Complete();
            Assert.IsFalse(status.Found);
        }
    }

    // Test that uses StartAddress parameter
    // (ref key, ref input, StartAddress,  userContext, serialNo, CancellationToken)
    [Test]
    [Category("TsavoriteKV")]
    public async Task AsyncStartAddressParamTest()
    {
        Status status;
        long key = default, input = default, output = default;

        long[] addresses = new long[numOps];
        long recordSize = store.Log.FixedRecordSize;

        using ClientSession<long, long, long, long, Empty, RMWSimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, RMWSimpleFunctions<long, long>>(new RMWSimpleFunctions<long, long>((a, b) => a + b));
        for (key = 0; key < numOps; key++)
        {
            // We can predict the address as TailAddress because we're single-threaded, *unless* a page was allocated;
            // in that case the new address is at the start of the newly-allocated page. Since we can't predict that,
            // we take advantage of knowing we have fixed-length records and that TailAddress is open-ended, so we
            // subtract after the insert to get record start address.
            (status, output) = (await s1.RMWAsync(ref key, ref key)).Complete();
            addresses[key] = store.Log.TailAddress - recordSize;
            Assert.IsFalse(status.IsPending);
            Assert.AreEqual(key, output);
        }

        ReadOptions readOptions;
        for (key = 0; key < numOps; key++)
        {
            readOptions = default;
            (status, output) = (await s1.ReadAtAddressAsync(addresses[key], ref key, ref output, ref readOptions)).Complete();
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key, output);
        }

        key = 0;
        input = 22;
        ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>> t1 = s1.RMWAsync(ref key, ref input);
        ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>> t2 = s1.RMWAsync(ref key, ref input);

        (await t1).Complete();
        (await t2).Complete(); // should trigger RMW re-do

        // Because of our small log-memory size, RMW of key 0 causes an RCW (Read-Copy-Write) and an insertion at the tail
        // of the log. Use the same pattern as above to get the new record address.
        addresses[key] = store.Log.TailAddress - recordSize;

        readOptions = default;
        (status, output) = (await s1.ReadAtAddressAsync(addresses[key], ref key, ref output, ref readOptions, Empty.Default, 129)).Complete();
        Assert.IsTrue(status.Found);
        Assert.AreEqual(key + input + input, output);
    }

    // Test of RMWAsync where No ref used
    [Test]
    [Category("TsavoriteKV")]
    public async Task ReadAsyncRMWAsyncNoRefTest()
    {
        Status status;
        long key = default, input = default, output = default;

        using ClientSession<long, long, long, long, Empty, RMWSimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, RMWSimpleFunctions<long, long>>(new RMWSimpleFunctions<long, long>((a, b) => a + b));
        for (key = 0; key < numOps; key++)
        {
            TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty> asyncResult = await (await s1.RMWAsync(key, key)).CompleteAsync();
            Assert.IsFalse(asyncResult.Status.IsPending);
            Assert.AreEqual(key, asyncResult.Output);
        }

        for (key = 0; key < numOps; key++)
        {
            (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key, output);
        }

        key = 0;
        input = 35;
        ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>> t1 = s1.RMWAsync(key, input);
        ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>> t2 = s1.RMWAsync(key, input);

        (await t1).Complete();
        (await t2).Complete(); // should trigger RMW re-do

        (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
        Assert.IsTrue(status.Found);
        Assert.AreEqual(key + input + input, output);
    }

    // Test of ReadyToCompletePendingAsync
    // Note: This should be looked into more to make it more of a "test" with proper verfication vs calling it to make sure just pop exception
    [Test]
    [Category("TsavoriteKV")]
    public async Task ReadyToCompletePendingAsyncTest()
    {
        Status status;
        long key = default, input = default, output = default;

        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>((a, b) => a + b));
        for (key = 0; key < numOps; key++)
        {
            (await s1.RMWAsync(key, key)).Complete();

            await s1.ReadyToCompletePendingAsync();
        }

        for (key = 0; key < numOps; key++)
        {
            (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key, output);
        }

        key = 0;
        input = 35;
        ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>> t1 = s1.RMWAsync(key, input);
        ValueTask<TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty>> t2 = s1.RMWAsync(key, input);

        (await t1).Complete();
        (await t2).Complete(); // should trigger RMW re-do

        (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
        Assert.IsTrue(status.Found);
        Assert.AreEqual(key + input + input, output);
    }

    // Test that does both UpsertAsync and RMWAsync to populate the TsavoriteKV and update it, possibly after flushing it from memory.
    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]

    public async Task UpsertAsyncAndRMWAsyncTest([Values] bool useRMW, [Values] bool doFlush, [Values] bool completeAsync)
    {
        using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> s1 = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());

        async ValueTask completeRmw(TsavoriteKV<long, long>.RmwAsyncResult<long, long, Empty> ar)
        {
            if (completeAsync)
            {
                while (ar.Status.IsPending)
                    ar = await ar.CompleteAsync(); // test async version of Upsert completion
                return;
            }
            ar.Complete();
        }

        async ValueTask completeUpsert(TsavoriteKV<long, long>.UpsertAsyncResult<long, long, Empty> ar)
        {
            if (completeAsync)
            {
                while (ar.Status.IsPending)
                    ar = await ar.CompleteAsync(); // test async version of Upsert completion
                return;
            }
            ar.Complete();
        }

        for (long key = 0; key < numOps; key++)
        {
            if (useRMW)
                await completeRmw(await s1.RMWAsync(key, key));
            else
                await completeUpsert(await s1.UpsertAsync(key, key));
        }

        if (doFlush)
            store.Log.FlushAndEvict(wait: true);

        for (long key = 0; key < numOps; key++)
        {
            if (useRMW)
                await completeRmw(await s1.RMWAsync(key, key + numOps));
            else
                await completeUpsert(await s1.UpsertAsync(key, key + numOps));
        }
    }
}