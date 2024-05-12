// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.Cluster.Tests;

internal class ClusterTestContext
{
    public CredentialManager credManager;
    public string TestFolder;
    public GarnetServer[] nodes = null;
    public EndPointCollection endpoints;
    public TextWriter logTextWriter = TestContext.Progress;
    public ILoggerFactory loggerFactory;
    public ILogger logger;

    public int defaultShards = 3;
    public static int Port = 7000;

    public Random r = new();
    public ManualResetEventSlim waiter;

    public Task checkpointTask;

    public ClusterTestUtils clusterTestUtils = null;

    public void Setup(HashSet<string> monitorTests)
    {
        TestFolder = TestUtils.UnitTestWorkingDir() + "\\";
        LogLevel logLevel = monitorTests.Contains(TestContext.CurrentContext.Test.MethodName) ? LogLevel.Trace : LogLevel.Error;
        loggerFactory = TestUtils.CreateLoggerFactoryInstance(logTextWriter, logLevel, scope: TestContext.CurrentContext.Test.Name);
        logger = loggerFactory.CreateLogger(TestContext.CurrentContext.Test.Name);
        logger.LogDebug("0. Setup >>>>>>>>>>>>");
        r = new Random(674386);
        waiter = new ManualResetEventSlim();
        credManager = new CredentialManager();
    }

    public void TearDown()
    {
        logger.LogDebug("0. Dispose <<<<<<<<<<<");
        waiter?.Dispose();
        clusterTestUtils?.Dispose();
        loggerFactory?.Dispose();
        DisposeCluster();
        TestUtils.DeleteDirectory(TestFolder, true);
    }

    /// <summary>
    /// Create instances with provided configuration
    /// </summary>
    public void CreateInstances(
        int shards,
        bool cleanClusterConfig = true,
        bool tryRecover = false,
        bool disableObjects = false,
        bool lowMemory = false,
        string MemorySize = default,
        string PageSize = default,
        string SegmentSize = "1g",
        bool enableAOF = false,
        bool MainMemoryReplication = false,
        bool OnDemandCheckpoint = false,
        string AofMemorySize = "64m",
        int CommitFrequencyMs = 0,
        bool DisableStorageTier = false,
        bool EnableIncrementalSnapshots = false,
        bool FastCommit = false,
        int timeout = -1,
        bool useTLS = false,
        bool useAcl = false,
        X509CertificateCollection certificates = null,
        ServerCredential clusterCreds = new ServerCredential())
    {
        endpoints = TestUtils.GetEndPoints(shards, 7000);
        nodes = TestUtils.CreateGarnetCluster(
            TestFolder,
            disablePubSub: true,
            disableObjects: disableObjects,
            endpoints: endpoints,
            enableAOF: enableAOF,
            timeout: timeout,
            loggerFactory: loggerFactory,
            tryRecover: tryRecover,
            UseTLS: useTLS,
            cleanClusterConfig: cleanClusterConfig,
            lowMemory: lowMemory,
            MemorySize: MemorySize,
            PageSize: PageSize,
            SegmentSize: SegmentSize,
            MainMemoryReplication: MainMemoryReplication,
            AofMemorySize: AofMemorySize,
            CommitFrequencyMs: CommitFrequencyMs,
            DisableStorageTier: DisableStorageTier,
            OnDemandCheckpoint: OnDemandCheckpoint,
            EnableIncrementalSnapshots: EnableIncrementalSnapshots,
            FastCommit: FastCommit,
            useAcl: useAcl,
            aclFile: credManager.aclFilePath,
            authUsername: clusterCreds.user,
            authPassword: clusterCreds.password,
            certificates: certificates);

        foreach (GarnetServer node in nodes)
            node.Start();
    }

    /// <summary>
    /// Create single cluster instance with corresponding options
    /// </summary>
    public GarnetServer CreateInstance(
        int Port,
        bool cleanClusterConfig = true,
        bool tryRecover = false,
        bool disableObjects = false,
        bool lowMemory = false,
        string MemorySize = default,
        string PageSize = default,
        string SegmentSize = "1g",
        bool enableAOF = false,
        bool MainMemoryReplication = false,
        bool OnDemandCheckpoint = false,
        string AofMemorySize = "64m",
        int CommitFrequencyMs = 0,
        bool DisableStorageTier = false,
        bool EnableIncrementalSnapshots = false,
        bool FastCommit = false,
        int timeout = -1,
        int gossipDelay = 5,
        bool useTLS = false,
        bool useAcl = false,
        X509CertificateCollection certificates = null,
        ServerCredential clusterCreds = new ServerCredential())
    {

        var opts = TestUtils.GetGarnetServerOptions(
            TestFolder,
            TestFolder,
            Port,
            disablePubSub: true,
            disableObjects: disableObjects,
            enableAOF: enableAOF,
            timeout: timeout,
            gossipDelay: gossipDelay,
            tryRecover: tryRecover,
            UseTLS: useTLS,
            cleanClusterConfig: cleanClusterConfig,
            lowMemory: lowMemory,
            MemorySize: MemorySize,
            PageSize: PageSize,
            SegmentSize: SegmentSize,
            MainMemoryReplication: MainMemoryReplication,
            AofMemorySize: AofMemorySize,
            CommitFrequencyMs: CommitFrequencyMs,
            DisableStorageTier: DisableStorageTier,
            OnDemandCheckpoint: OnDemandCheckpoint,
            EnableIncrementalSnapshots: EnableIncrementalSnapshots,
            FastCommit: FastCommit,
            useAcl: useAcl,
            aclFile: credManager.aclFilePath,
            authUsername: clusterCreds.user,
            authPassword: clusterCreds.password,
            certificates: certificates);

        return new GarnetServer(opts, loggerFactory);
    }


    /// <summary>
    /// Dispose created instances
    /// </summary>
    public void DisposeCluster()
    {
        if (nodes != null)
        {
            for (int i = 0; i < nodes.Length; i++)
            {
                if (nodes[i] != null)
                {
                    logger.LogDebug("\t a. Dispose node {testName}", TestContext.CurrentContext.Test.Name);
                    nodes[i].Dispose();
                    nodes[i] = null;
                    logger.LogDebug("\t b. Dispose node {testName}", TestContext.CurrentContext.Test.Name);
                }
            }
        }
    }

    /// <summary>
    /// Establish connection to cluster.
    /// </summary>
    public void CreateConnection(
        bool useTLS = false,
        X509CertificateCollection certificates = null,
        ServerCredential clientCreds = new ServerCredential())
    {
        clusterTestUtils?.Dispose();
        clusterTestUtils = new ClusterTestUtils(
            endpoints,
            textWriter: logTextWriter,
            UseTLS: useTLS,
            authUsername: clientCreds.user,
            authPassword: clientCreds.password,
            certificates: certificates);
        clusterTestUtils.Connect(logger);
        clusterTestUtils.PingAll(logger);
    }

    /// <summary>
    /// Generate credential file through credManager
    /// </summary>
    public void GenerateCredentials(ServerCredential[] customCreds = null)
        => credManager.GenerateCredentials(TestFolder, customCreds);

    public int keyOffset = 0;
    public bool orderedKeys = false;

    public Dictionary<string, int> kvPairs;
    public Dictionary<string, List<int>> kvPairsObj;

    public void PopulatePrimary(
        ref Dictionary<string, int> kvPairs,
        int keyLength,
        int kvpairCount,
        int primaryIndex,
        int[] slotMap = null,
        bool incrementalSnapshots = false,
        int ckptNode = 0,
        int randomSeed = -1)
    {
        if (randomSeed != -1) clusterTestUtils.InitRandom(randomSeed);
        for (int i = 0; i < kvpairCount; i++)
        {
            string key = orderedKeys ? keyOffset++.ToString() : clusterTestUtils.RandomStr(keyLength);
            int value = r.Next();

            //Use slotMap
            byte[] keyBytes = Encoding.ASCII.GetBytes(key);
            if (slotMap != null)
            {
                ushort slot = ClusterTestUtils.HashSlot(keyBytes);
                primaryIndex = slotMap[slot];
            }

            ResponseState resp = clusterTestUtils.SetKey(primaryIndex, keyBytes, Encoding.ASCII.GetBytes(value.ToString()), out int _, out string _, out int _, logger: logger);
            Assert.AreEqual(ResponseState.OK, resp);

            string retVal = clusterTestUtils.GetKey(primaryIndex, keyBytes, out int _, out string _, out int _, out ResponseState responseState, logger: logger);
            Assert.AreEqual(ResponseState.OK, responseState);
            Assert.AreEqual(value, int.Parse(retVal));

            kvPairs.Add(key, int.Parse(retVal));

            if (incrementalSnapshots && i == kvpairCount / 2)
                clusterTestUtils.Checkpoint(ckptNode, logger: logger);
        }
    }

    public void PopulatePrimaryRMW(ref Dictionary<string, int> kvPairs, int keyLength, int kvpairCount, int primaryIndex, int addCount, int[] slotMap = null, bool incrementalSnapshots = false, int ckptNode = 0, int randomSeed = -1)
    {
        if (randomSeed != -1) clusterTestUtils.InitRandom(randomSeed);
        for (int i = 0; i < kvpairCount; i++)
        {
            string key = orderedKeys ? keyOffset++.ToString() : clusterTestUtils.RandomStr(keyLength);

            // Use slotMap
            byte[] keyBytes = Encoding.ASCII.GetBytes(key);
            if (slotMap != null)
            {
                ushort slot = ClusterTestUtils.HashSlot(keyBytes);
                primaryIndex = slotMap[slot];
            }

            int value = 0;
            for (int j = 0; j < addCount; j++)
                value = clusterTestUtils.IncrBy(primaryIndex, key, randomSeed == -1 ? 1 : clusterTestUtils.r.Next(1, 100));

            kvPairs.Add(key, value);

            if (incrementalSnapshots && i == kvpairCount / 2)
                clusterTestUtils.Checkpoint(ckptNode, logger: logger);
        }
    }

    public void PopulatePrimaryWithObjects(ref Dictionary<string, List<int>> kvPairsObj, int keyLength, int kvpairCount, int primaryIndex, int countPerList = 32, int itemSize = 1 << 20, int randomSeed = -1, bool set = false)
    {
        if (randomSeed != -1) clusterTestUtils.InitRandom(randomSeed);
        for (int i = 0; i < kvpairCount; i++)
        {
            string key = clusterTestUtils.RandomStr(keyLength);
            List<int> value = !set ? clusterTestUtils.RandomList(countPerList, itemSize) : clusterTestUtils.RandomHset(countPerList, itemSize);
            while (kvPairsObj.ContainsKey(key))
                key = clusterTestUtils.RandomStr(keyLength);
            kvPairsObj.Add(key, value);

            if (!set)
                clusterTestUtils.Lpush(primaryIndex, key, value, logger);
            else
                clusterTestUtils.Sadd(primaryIndex, key, value, logger);

            if (!set)
            {
                List<int> result = clusterTestUtils.Lrange(primaryIndex, key, logger);
                Assert.AreEqual(value, result);
            }
            else
            {
                List<int> result = clusterTestUtils.Smembers(primaryIndex, key, logger);
                Assert.IsTrue(result.ToHashSet().SetEquals(value.ToHashSet()));
            }
        }
    }

    public void PopulatePrimaryAndTakeCheckpointTask(bool performRMW, bool disableObjects, bool takeCheckpoint, int iter = 5)
    {
        int keyLength = 32;
        int kvpairCount = 64;
        int addCount = 5;
        for (int i = 0; i < iter; i++)
        {
            // Populate Primary
            if (disableObjects)
            {
                if (!performRMW)
                    PopulatePrimary(ref kvPairs, keyLength, kvpairCount, 0);
                else
                    PopulatePrimaryRMW(ref kvPairs, keyLength, kvpairCount, 0, addCount);
            }
            else
            {
                PopulatePrimaryWithObjects(ref kvPairsObj, keyLength, kvpairCount, 0);
            }
            if (takeCheckpoint) clusterTestUtils.Checkpoint(0, logger: logger);
        }
    }

    public void ValidateKVCollectionAgainstReplica(
        ref Dictionary<string, int> kvPairs,
        int replicaIndex,
        int primaryIndex = 0,
        int[] slotMap = null)
    {
        IEnumerable<string> keys = orderedKeys ? kvPairs.Keys.Select(int.Parse).ToList().OrderBy(x => x).Select(x => x.ToString()) : kvPairs.Keys;
        foreach (string key in keys)
        {
            int value = kvPairs[key];
            byte[] keyBytes = Encoding.ASCII.GetBytes(key);

            if (slotMap != null)
            {
                ushort slot = ClusterTestUtils.HashSlot(keyBytes);
                replicaIndex = slotMap[slot];
            }

            string retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out int _, out string _, out int _, out ResponseState responseState, logger: logger);
            while (retVal == null || (value != int.Parse(retVal)))
            {
                retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out int _, out string _, out int _, out responseState, logger: logger);
                ClusterTestUtils.BackOff();
            }
            Assert.AreEqual(ResponseState.OK, responseState);
            Assert.AreEqual(value, int.Parse(retVal), $"replOffset > p:{clusterTestUtils.GetReplicationOffset(primaryIndex, logger: logger)}, s[{replicaIndex}]:{clusterTestUtils.GetReplicationOffset(replicaIndex)}");
        }
    }

    public void ValidateNodeObjects(ref Dictionary<string, List<int>> kvPairsObj, int nodeIndex, bool set = false)
    {
        foreach (string key in kvPairsObj.Keys)
        {
            List<int> elements = kvPairsObj[key];
            List<int> result;
            if (!set)
                result = clusterTestUtils.Lrange(nodeIndex, key, logger);
            else
                result = clusterTestUtils.Smembers(nodeIndex, key, logger);

            while (result.Count == 0)
            {
                if (!set)
                    result = clusterTestUtils.Lrange(nodeIndex, key, logger);
                else
                    result = clusterTestUtils.Smembers(nodeIndex, key, logger);
                ClusterTestUtils.BackOff();
            }
            if (!set)
                Assert.AreEqual(elements, result);
            else
                Assert.IsTrue(result.ToHashSet().SetEquals(result.ToHashSet()));
        }
    }

    public void SendAndValidateKeys(int primaryIndex, int replicaIndex, int keyLength, int numKeys = 1)
    {
        for (int i = 0; i < numKeys; i++)
        {
            string key = orderedKeys ? keyOffset++.ToString() : clusterTestUtils.RandomStr(keyLength);
            byte[] keyBytes = Encoding.ASCII.GetBytes(key);
            int value = r.Next();
            ResponseState resp = clusterTestUtils.SetKey(primaryIndex, keyBytes, Encoding.ASCII.GetBytes(value.ToString()), out int _, out string _, out int _, logger: logger);
            Assert.AreEqual(ResponseState.OK, resp);

            clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex);

            string retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out int _, out string _, out int _, out ResponseState responseState, logger: logger);
            while (retVal == null || (value != int.Parse(retVal)))
            {
                retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out int _, out string _, out int _, out responseState, logger: logger);
                ClusterTestUtils.BackOff();
            }
            Assert.AreEqual(ResponseState.OK, responseState);
            Assert.AreEqual(value, int.Parse(retVal), $"replOffset > p:{clusterTestUtils.GetReplicationOffset(primaryIndex, logger: logger)}, s[{replicaIndex}]:{clusterTestUtils.GetReplicationOffset(replicaIndex)}");
        }
    }

    public void ClusterFailoveSpinWait(int replicaNodeIndex, ILogger logger)
    {
        // Failover primary
        _ = clusterTestUtils.ClusterFailover(replicaNodeIndex, "ABORT", logger);
        _ = clusterTestUtils.ClusterFailover(replicaNodeIndex, logger: logger);

        int retryCount = 0;
        while (true)
        {
            string role = clusterTestUtils.GetReplicationRole(replicaNodeIndex, logger: logger);
            if (role.Equals("master")) break;
            if (retryCount++ > 10000)
            {
                logger?.LogError("CLUSTER FAILOVER retry count reached");
                Assert.Fail();
            }
            Thread.Sleep(1000);
        }
    }

    public void AttachAndWaitForSync(int primary_count, int replica_count, bool disableObjects)
    {
        string primaryId = clusterTestUtils.GetNodeIdFromNode(0, logger);
        // Issue meet to replicas
        for (int i = primary_count; i < primary_count + replica_count; i++)
            clusterTestUtils.Meet(i, 0);

        // Wait until primary node is known so as not to fail replicate
        for (int i = primary_count; i < primary_count + replica_count; i++)
            clusterTestUtils.WaitUntilNodeIdIsKnown(i, primaryId, logger: logger);

        // Issue cluster replicate and bump epoch manually to capture config.
        for (int i = primary_count; i < primary_count + replica_count; i++)
        {
            _ = clusterTestUtils.ClusterReplicate(i, primaryId, async: true, logger: logger);
            clusterTestUtils.BumpEpoch(i, logger: logger);
        }

        if (!checkpointTask.Wait(TimeSpan.FromSeconds(100))) Assert.Fail("Checkpoint task timeout");

        // Wait for recovery and AofSync
        for (int i = primary_count; i < replica_count; i++)
        {
            clusterTestUtils.WaitForReplicaRecovery(i, logger);
            clusterTestUtils.WaitForReplicaAofSync(0, i, logger);
        }

        clusterTestUtils.WaitForConnectedReplicaCount(0, replica_count, logger: logger);

        // Validate data on replicas
        for (int i = primary_count; i < replica_count; i++)
        {
            if (disableObjects)
                ValidateKVCollectionAgainstReplica(ref kvPairs, i);
            else
                ValidateNodeObjects(ref kvPairsObj, i);
        }
    }
}