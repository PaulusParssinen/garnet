// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.Device;
using Tsavorite.Tests.Recovery;

namespace Tsavorite.Tests;

public class CheckpointManagerTests
{
    private Random random = new Random(0);

    [Test]
    [Category("CheckpointRestore")]
    [Category("Smoke")]
    public async Task CheckpointManagerPurgeCheck([Values] DeviceMode deviceMode)
    {
        ICheckpointManager checkpointManager;
        checkpointManager = new DeviceLogCommitCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme(Path.Join(TestUtils.MethodTestDir, "checkpoints")), false); // PurgeAll deletes this directory
        
        using (IDevice log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "hlog.log"), deleteOnClose: true))
        {
            TestUtils.RecreateDirectory(TestUtils.MethodTestDir);

            using var store = new TsavoriteKV<long, long>
            (1 << 10,
                logSettings: new LogSettings
                {
                    LogDevice = log,
                    MutableFraction = 1,
                    PageSizeBits = 10,
                    MemorySizeBits = 20,
                    ReadCacheSettings = null
                },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
            );
            using ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> s = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());

            var logCheckpoints = new Dictionary<Guid, int>();
            var indexCheckpoints = new Dictionary<Guid, int>();
            var fullCheckpoints = new Dictionary<Guid, int>();

            for (int i = 0; i < 10; i++)
            {
                // Do some dummy update
                s.Upsert(0, random.Next());

                int checkpointType = random.Next(5);
                Guid result = default;
                switch (checkpointType)
                {
                    case 0:
                        store.TryInitiateHybridLogCheckpoint(out result, CheckpointType.FoldOver);
                        logCheckpoints.Add(result, 0);
                        break;
                    case 1:
                        store.TryInitiateHybridLogCheckpoint(out result, CheckpointType.Snapshot);
                        logCheckpoints.Add(result, 0);
                        break;
                    case 2:
                        store.TryInitiateIndexCheckpoint(out result);
                        indexCheckpoints.Add(result, 0);
                        break;
                    case 3:
                        store.TryInitiateFullCheckpoint(out result, CheckpointType.FoldOver);
                        fullCheckpoints.Add(result, 0);
                        break;
                    case 4:
                        store.TryInitiateFullCheckpoint(out result, CheckpointType.Snapshot);
                        fullCheckpoints.Add(result, 0);
                        break;
                    default:
                        Assert.True(false);
                        break;
                }

                await store.CompleteCheckpointAsync();
            }

            Assert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
            Assert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));

            if (logCheckpoints.Count != 0)
            {
                Guid guid = logCheckpoints.First().Key;
                checkpointManager.Purge(guid);
                logCheckpoints.Remove(guid);
                Assert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                Assert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
            }

            if (indexCheckpoints.Count != 0)
            {
                Guid guid = indexCheckpoints.First().Key;
                checkpointManager.Purge(guid);
                indexCheckpoints.Remove(guid);
                Assert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                Assert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
            }


            if (fullCheckpoints.Count != 0)
            {
                Guid guid = fullCheckpoints.First().Key;
                checkpointManager.Purge(guid);
                fullCheckpoints.Remove(guid);
                Assert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                Assert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
            }

            checkpointManager.PurgeAll();
            Assert.IsEmpty(checkpointManager.GetLogCheckpointTokens());
            Assert.IsEmpty(checkpointManager.GetIndexCheckpointTokens());
        }
        checkpointManager.Dispose();
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
    }
}