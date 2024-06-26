﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.Device;

namespace Tsavorite.Tests.Recovery.objectstore;

internal struct StructTuple<T1, T2>
{
    public T1 Item1;
    public T2 Item2;
}

[TestFixture]
internal class ObjectRecoveryTests
{
    private const long numUniqueKeys = 1 << 14;
    private const long keySpace = 1L << 14;
    private const long numOps = 1L << 19;
    private const long completePendingInterval = 1L << 10;
    private const long checkpointInterval = 1L << 16;
    private TsavoriteKV<AdId, NumClicks> store;
    private Guid token;
    private IDevice log, objlog;

    [SetUp]
    public void Setup() => Setup(deleteDir: true);

    public void Setup(bool deleteDir)
    {
        if (deleteDir)
            TestUtils.RecreateDirectory(TestUtils.MethodTestDir);

        log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "ObjectRecoveryTests.log"), false);
        objlog = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "ObjectRecoveryTests.obj.log"), false);

        store = new TsavoriteKV<AdId, NumClicks>
            (
                keySpace,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog },
                new CheckpointSettings { CheckpointDir = TestUtils.MethodTestDir },
                new SerializerSettings<AdId, NumClicks> { keySerializer = () => new AdIdSerializer(), valueSerializer = () => new NumClicksSerializer() }
                );
    }

    [TearDown]
    public void TearDown() => TearDown(deleteDir: true);

    public void TearDown(bool deleteDir)
    {
        store?.Dispose();
        store = null;
        log?.Dispose();
        log = null;
        objlog?.Dispose();
        objlog = null;

        if (deleteDir)
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
    }

    private void PrepareToRecover()
    {
        TearDown(deleteDir: false);
        Setup(deleteDir: false);
    }

    [Test]
    [Category("TsavoriteKV"), Category("CheckpointRestore")]
    public async ValueTask ObjectRecoveryTest1([Values] bool isAsync)
    {
        Populate();
        PrepareToRecover();

        if (isAsync)
            await store.RecoverAsync(token, token);
        else
            store.Recover(token, token);

        Verify(token, token);
    }

    public unsafe void Populate()
    {
        // Prepare the dataset
        var inputArray = new StructTuple<AdId, Input>[numOps];
        for (int i = 0; i < numOps; i++)
        {
            inputArray[i] = new StructTuple<AdId, Input>
            {
                Item1 = new AdId { adId = i % numUniqueKeys },
                Item2 = new Input { numClicks = new NumClicks { numClicks = 1 } }
            };
        }

        // Register thread with Tsavorite
        ClientSession<AdId, NumClicks, Input, Output, Empty, Functions> session = store.NewSession<Input, Output, Empty, Functions>(new Functions());

        // Process the batch of input data
        bool first = true;
        for (int i = 0; i < numOps; i++)
        {
            session.RMW(ref inputArray[i].Item1, ref inputArray[i].Item2, Empty.Default, i);

            if ((i + 1) % checkpointInterval == 0)
            {
                if (first)
                    while (!store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot)) ;
                else
                    while (!store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot)) ;

                store.CompleteCheckpointAsync().GetAwaiter().GetResult();

                first = false;
            }

            if (i % completePendingInterval == 0)
            {
                session.CompletePending(false, false);
            }
        }


        // Make sure operations are completed
        session.CompletePending(true);
        session.Dispose();
    }

    public unsafe void Verify(Guid cprVersion, Guid indexVersion)
    {
        // Create array for reading
        var inputArray = new StructTuple<AdId, Input>[numUniqueKeys];
        for (int i = 0; i < numUniqueKeys; i++)
        {
            inputArray[i] = new StructTuple<AdId, Input>
            {
                Item1 = new AdId { adId = i },
                Item2 = new Input { numClicks = new NumClicks { numClicks = 0 } }
            };
        }

        var outputArray = new Output[numUniqueKeys];
        for (int i = 0; i < numUniqueKeys; i++)
        {
            outputArray[i] = new Output();
        }

        // Register with thread
        ClientSession<AdId, NumClicks, Input, Output, Empty, Functions> session = store.NewSession<Input, Output, Empty, Functions>(new Functions());

        Input input = default;
        // Issue read requests
        for (int i = 0; i < numUniqueKeys; i++)
        {
            session.Read(ref inputArray[i].Item1, ref input, ref outputArray[i], Empty.Default, i);
        }

        // Complete all pending requests
        session.CompletePending(true);

        // Release
        session.Dispose();

        // Test outputs
        var checkpointInfo = default(HybridLogRecoveryInfo);
        checkpointInfo.Recover(cprVersion,
            new DeviceLogCommitCheckpointManager(
                new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(
                      new DirectoryInfo(TestUtils.MethodTestDir).FullName)), null);

        // Compute expected array
        long[] expected = new long[numUniqueKeys];
        foreach (int guid in checkpointInfo.continueTokens.Keys)
        {
            CommitPoint cp = checkpointInfo.continueTokens[guid].Item2;
            for (long i = 0; i <= cp.UntilSerialNo; i++)
            {
                long id = i % numUniqueKeys;
                expected[id]++;
            }
        }

        int threadCount = 1; // single threaded test
        int numCompleted = threadCount - checkpointInfo.continueTokens.Count;
        for (int t = 0; t < numCompleted; t++)
        {
            long sno = numOps;
            for (long i = 0; i < sno; i++)
            {
                long id = i % numUniqueKeys;
                expected[id]++;
            }
        }

        // Assert if expected is same as found
        for (long i = 0; i < numUniqueKeys; i++)
        {
            Assert.AreEqual(expected[i], outputArray[i].value.numClicks, $"AdId {inputArray[i].Item1.adId}");
        }
    }
}