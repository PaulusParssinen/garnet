﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.Tests;

[TestFixture]
public class RespMetricsTest
{
    private GarnetServer server;
    private ILoggerFactory loggerFactory;
    private Random r;

    private void StartServer(int metricsSamplingFreq = -1, bool latencyMonitor = false)
    {
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, metricsSamplingFreq: metricsSamplingFreq, latencyMonitor: latencyMonitor);
        server.Start();
    }

    [SetUp]
    public void Setup()
    {
        server = null;
        r = new Random(674386);
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        loggerFactory = TestUtils.CreateLoggerFactoryInstance(TestContext.Progress, LogLevel.Error);
    }

    [TearDown]
    public void TearDown()
    {
        server?.Dispose();
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
    }

    [Test]
    public void MetricsDisabledTest()
    {
        StartServer();
        ILogger logger = loggerFactory.CreateLogger(TestContext.CurrentContext.Test.Name);
        (InfoMetricsType, MetricsItem[])[] infoMetrics = server.Metrics.GetInfoMetrics().ToArray();

        Assert.AreNotEqual(null, infoMetrics);
        foreach ((InfoMetricsType, MetricsItem[]) section in infoMetrics)
        {
            logger.LogDebug("<{sectionName}>", section.Item1);
            foreach (MetricsItem prop in section.Item2)
            {
                if (section.Item1 == InfoMetricsType.STATS)
                {
                    if (prop.Name.Equals("garnet_hit_rate"))
                        Assert.AreEqual("0.00", prop.Value);
                    else
                        Assert.AreEqual("0", prop.Value);
                }
                logger.LogDebug("\t {propName} : {propValue}", prop.Name, prop.Value);
            }
            logger.LogDebug("</{sectionName}>", section.Item1);
        }

        (LatencyMetricsType, MetricsItem[])[] latencyMetrics = server.Metrics.GetLatencyMetrics().ToArray();
        Assert.AreEqual(Array.Empty<(LatencyMetricsType, MetricsItem[])>(), latencyMetrics);
    }

    [Test]
    public void MetricsEnabledTest()
    {
        StartServer(1, true);
        ILogger logger = loggerFactory.CreateLogger(TestContext.CurrentContext.Test.Name);
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        int opCount = 1000;
        for (int i = 0; i < opCount; i++)
        {
            Assert.IsTrue(db.StringSet(i.ToString(), i.ToString()));
            string result = (string)db.StringGet(i.ToString());
            Assert.AreEqual(i.ToString(), result);
        }

        bool first = true;
    retry:
        Thread.Sleep(2000);
        (InfoMetricsType, MetricsItem[])[] infoMetrics = server.Metrics.GetInfoMetrics().ToArray();
        Assert.AreNotEqual(null, infoMetrics);
        foreach ((InfoMetricsType, MetricsItem[]) section in infoMetrics)
        {
            logger.LogDebug("<{sectionName}>", section.Item1);
            foreach (MetricsItem prop in section.Item2)
            {
                if (section.Item1 == InfoMetricsType.STATS)
                {
                    if (prop.Name.Equals("total_commands_processed"))
                    {
                        int total_commands_processed = int.Parse(prop.Value);
                        if (first && total_commands_processed < opCount)
                        {
                            first = false;
                            goto retry;
                        }
                        Assert.GreaterOrEqual(total_commands_processed, opCount);
                    }
                }
                logger.LogDebug("\t {propName} : {propValue}", prop.Name, prop.Value);
            }
            logger.LogDebug("</{sectionName}>", section.Item1);
        }

        MetricsItem[] latencyMetrics = server.Metrics.GetLatencyMetrics(LatencyMetricsType.NET_RS_LAT).ToArray();
        while (latencyMetrics.Length == 0)
        {
            Thread.Yield();
            latencyMetrics = server.Metrics.GetLatencyMetrics(LatencyMetricsType.NET_RS_LAT).ToArray();
        }
        Assert.AreNotEqual(Array.Empty<(LatencyMetricsType, MetricsItem[])>(), latencyMetrics);
        Assert.AreEqual(8, latencyMetrics.Length);

        Assert.AreEqual("calls", latencyMetrics[0].Name);
        Assert.AreEqual("min", latencyMetrics[1].Name);
        Assert.AreEqual("5th", latencyMetrics[2].Name);
        Assert.AreEqual("50th", latencyMetrics[3].Name);
        Assert.AreEqual("mean", latencyMetrics[4].Name);
        Assert.AreEqual("95th", latencyMetrics[5].Name);
        Assert.AreEqual("99th", latencyMetrics[6].Name);
        Assert.AreEqual("99.9th", latencyMetrics[7].Name);

    }
}