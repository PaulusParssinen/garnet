// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.Cluster.Tests;

[TestFixture, NonParallelizable]
public class ClusterManagementTests
{
    private ClusterTestContext context;
    private readonly int defaultShards = 3;
    private readonly HashSet<string> monitorTests = [];

    [SetUp]
    public void Setup()
    {
        context = new ClusterTestContext();
        context.Setup(monitorTests);
    }

    [TearDown]
    public void TearDown()
    {
        context.TearDown();
    }

    [Test, Order(1)]
    [TestCase(0, 16383)]
    [TestCase(1234, 5678)]
    public void ClusterSlotsTest(int startSlot, int endSlot)
    {
        var slotRanges = new List<(int, int)>[1];
        slotRanges[0] = [(startSlot, endSlot)];
        context.CreateInstances(defaultShards);
        context.CreateConnection();
        _ = context.clusterTestUtils.SimpleSetupCluster(customSlotRanges: slotRanges, logger: context.logger);

        List<SlotItem> slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
        Assert.IsTrue(slotsResult.Count == 1);
        Assert.AreEqual(startSlot, slotsResult[0].startSlot);
        Assert.AreEqual(endSlot, slotsResult[0].endSlot);
        Assert.IsTrue(slotsResult[0].nnInfo.Length == 1);
        Assert.IsTrue(slotsResult[0].nnInfo[0].isPrimary);
        Assert.AreEqual(slotsResult[0].nnInfo[0].address, context.clusterTestUtils.GetEndPoint(0).Address.ToString());
        Assert.AreEqual(slotsResult[0].nnInfo[0].port, context.clusterTestUtils.GetEndPoint(0).Port);
        Assert.AreEqual(slotsResult[0].nnInfo[0].nodeid, context.clusterTestUtils.GetNodeIdFromNode(0, context.logger));
    }

    [Test, Order(2)]
    public void ClusterSlotRangesTest()
    {
        context.CreateInstances(defaultShards);
        context.CreateConnection();
        var slotRanges = new List<(int, int)>[3];
        slotRanges[0] = [(5680, 6150), (12345, 14567)];
        slotRanges[1] = [(1021, 2371), (3376, 5678)];
        slotRanges[2] = [(782, 978), (7345, 11819)];
        _ = context.clusterTestUtils.SimpleSetupCluster(customSlotRanges: slotRanges, logger: context.logger);

        List<SlotItem> slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
        while (slotsResult.Count < 6)
            slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
        Assert.AreEqual(6, slotsResult.Count);

        List<(int, (int, int))>[] origSlotRanges = new List<(int, (int, int))>[3];
        for (int i = 0; i < slotRanges.Length; i++)
        {
            origSlotRanges[i] = new List<(int, (int, int))>();
            for (int j = 0; j < slotRanges[i].Count; j++)
                origSlotRanges[i].Add((i, slotRanges[i][j]));
        }
        var ranges = origSlotRanges.SelectMany(x => x).OrderBy(x => x.Item2.Item1).ToList();
        Assert.IsTrue(slotsResult.Count == ranges.Count);
        for (int i = 0; i < slotsResult.Count; i++)
        {
            (int, (int, int)) origRange = ranges[i];
            SlotItem retRange = slotsResult[i];
            Assert.AreEqual(origRange.Item2.Item1, retRange.startSlot);
            Assert.AreEqual(origRange.Item2.Item2, retRange.endSlot);
            Assert.IsTrue(retRange.nnInfo.Length == 1);
            Assert.IsTrue(retRange.nnInfo[0].isPrimary);
            Assert.AreEqual(context.clusterTestUtils.GetEndPoint(origRange.Item1).Address.ToString(), retRange.nnInfo[0].address);
            Assert.AreEqual(context.clusterTestUtils.GetEndPoint(origRange.Item1).Port, retRange.nnInfo[0].port);
            Assert.AreEqual(context.clusterTestUtils.GetNodeIdFromNode(origRange.Item1, context.logger), retRange.nnInfo[0].nodeid);
        }
    }

    [Test, Order(3)]
    public void ClusterForgetTest()
    {
        int node_count = 4;
        context.CreateInstances(node_count);
        context.CreateConnection();
        (List<ShardInfo> _, List<ushort> _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);

        string[] nodeIds = context.clusterTestUtils.GetNodeIds(logger: context.logger);

        // Forget node0
        for (int i = 1; i < node_count; i++)
        {
            // Issue forget node i to node 0 for 30 seconds
            _ = context.clusterTestUtils.ClusterForget(0, nodeIds[i], 30, context.logger);
            // Issue forget node 0 to node i
            _ = context.clusterTestUtils.ClusterForget(i, nodeIds[0], 30, context.logger);
        }

        // Retrieve config for nodes 1 to i-1
        List<ClusterConfiguration> configs = new();
        for (int i = 1; i < node_count; i++)
            configs.Add(context.clusterTestUtils.ClusterNodes(i, context.logger));

        // Check if indeed nodes 1 to i-1 have forgotten node 0
        foreach (ClusterConfiguration config in configs)
            foreach (ClusterNode node in config.Nodes)
                Assert.AreNotEqual(nodeIds[0], node.NodeId, "node 0 node forgotten");
    }

    [Test, Order(4)]
    public void ClusterResetTest()
    {
        int node_count = 4;
        context.CreateInstances(node_count);
        context.CreateConnection();
        (List<ShardInfo> _, List<ushort> _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);

        // Get slot ranges for node 0
        ClusterConfiguration config = context.clusterTestUtils.ClusterNodes(0, context.logger);
        IList<SlotRange> slots = config.Nodes.First().Slots;
        List<(int, int)> slotRanges = new();
        foreach (SlotRange slot in slots)
            slotRanges.Add((slot.From, slot.To));

        string[] nodeIds = context.clusterTestUtils.GetNodeIds(logger: context.logger);
        // Issue forget of node 0 to nodes 1 to i-1
        for (int i = 1; i < node_count; i++)
            _ = context.clusterTestUtils.ClusterForget(i, nodeIds[0], 10, context.logger);

        try
        {
            // Add data to server
            RedisResult resp = context.clusterTestUtils.GetServer(0).Execute("SET", "wxz", "1234");
            Assert.AreEqual("OK", (string)resp);

            resp = context.clusterTestUtils.GetServer(0).Execute("GET", "wxz");
            Assert.AreEqual("1234", (string)resp);
        }
        catch (Exception ex)
        {
            context.logger?.LogError(ex, "An error occured at ClusterResetTest");
        }

        // Hard reset node state. clean db data and cluster config
        _ = context.clusterTestUtils.ClusterReset(0, soft: false, 10, context.logger);
        config = context.clusterTestUtils.ClusterNodes(0, context.logger);
        ClusterNode node = config.Nodes.First();

        // Assert node 0 does not know anything about the cluster
        Assert.AreEqual(1, config.Nodes.Count);
        Assert.AreNotEqual(nodeIds[0], node.NodeId);
        Assert.AreEqual(0, node.Slots.Count);
        Assert.IsFalse(node.IsReplica);

        //Add slotRange for clean node
        context.clusterTestUtils.AddSlotsRange(0, slotRanges, context.logger);
        try
        {
            // Check DB was flushed due to hard reset
            RedisResult resp = context.clusterTestUtils.GetServer(0).Execute("GET", "wxz");
            Assert.IsTrue(resp.IsNull, "DB not flushed after HARD reset");

            // Add data to server
            resp = context.clusterTestUtils.GetServer(0).Execute("SET", "wxz", "1234");
            Assert.AreEqual("OK", (string)resp);

            resp = context.clusterTestUtils.GetServer(0).Execute("GET", "wxz");
            Assert.AreEqual("1234", (string)resp);
        }
        catch (Exception ex)
        {
            context.logger?.LogError(ex, "An error occured at ClusterResetTest");
        }

        // Add node back to the cluster
        context.clusterTestUtils.SetConfigEpoch(0, 1, context.logger);
        context.clusterTestUtils.Meet(0, 1, context.logger);

        context.clusterTestUtils.WaitUntilNodeIsKnownByAllNodes(0, context.logger);
    }

    //[Test, Order(5)]
    //[Category("CLUSTER")]
    public void ClusterRestartNodeDropGossip()
    {
        ILogger logger = context.loggerFactory.CreateLogger("ClusterRestartNodeDropGossip");
        context.CreateInstances(defaultShards);
        context.CreateConnection();
        (List<ShardInfo> _, List<ushort> _) = context.clusterTestUtils.SimpleSetupCluster(logger: logger);

        int restartingNode = 2;
        // Dispose node and delete data
        context.nodes[restartingNode].Dispose(deleteDir: true);

        context.nodes[restartingNode] = context.CreateInstance(
            context.clusterTestUtils.GetEndPoint(restartingNode).Port,
            disableObjects: true,
            tryRecover: false,
            enableAOF: true,
            timeout: 60,
            gossipDelay: 1,
            cleanClusterConfig: false);
        context.nodes[restartingNode].Start();
        context.CreateConnection();

        Thread.Sleep(5000);
        for (int i = 0; i < 2; i++)
        {
            ClusterConfiguration config = context.clusterTestUtils.ClusterNodes(restartingNode, logger: logger);
            ClusterNode[] knownNodes = config.Nodes.ToArray();
            Assert.AreEqual(knownNodes.Length, 1);
            Thread.Sleep(1000);
        }
    }
}