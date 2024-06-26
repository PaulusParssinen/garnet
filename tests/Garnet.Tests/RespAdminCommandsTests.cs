﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.Tests;

[TestFixture]
public class RespAdminCommandsTests
{
    private GarnetServer server;

    [SetUp]
    public void Setup()
    {
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
        server.Start();
    }

    [TearDown]
    public void TearDown()
    {
        server.Dispose();
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
    }

    #region LightclientTests

    [Test]
    public void PingTest()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        string expectedResponse = "+PONG\r\n";
        var response = lightClientRequest.SendCommand("PING");
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void PingMessageTest()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        string expectedResponse = "$5\r\nHELLO\r\n";
        var response = lightClientRequest.SendCommand("PING HELLO");
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void PingErrorMessageTest()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        string expectedResponse = "-ERR wrong number of arguments for 'ping' command\r\n";
        var response = lightClientRequest.SendCommand("PING HELLO WORLD", 1);
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void EchoWithNoMessageReturnErrorTest()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        string expectedResponse = "-ERR wrong number of arguments for 'echo' command\r\n";
        var response = lightClientRequest.SendCommand("ECHO", 1);
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }


    [Test]
    public void EchoWithMessagesReturnErrorTest()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        string expectedResponse = "-ERR wrong number of arguments for 'echo' command\r\n";
        var response = lightClientRequest.SendCommand("ECHO HELLO WORLD", 1);
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
        response = lightClientRequest.SendCommand("ECHO HELLO WORLD WORLD2", 1);
        actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void EchoWithMessageTest()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        string expectedResponse = "$5\r\nHELLO\r\n";
        var response = lightClientRequest.SendCommand("ECHO HELLO", 1);
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void EchoTwoCommandsTest()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        string expectedResponse = "-ERR wrong number of arguments for 'echo' command\r\n$5\r\nHELLO\r\n";
        var response = lightClientRequest.SendCommands("ECHO HELLO WORLD WORLD2", "ECHO HELLO", 1, 1);
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }


    [Test]
    public void TimeCommandTest()
    {
        // this is an example, we just compare the length of the response with the expected one.
        using var lightClientRequest = TestUtils.CreateRequest();
        string expectedResponse = "*2\r\n$10\r\n1626282789\r\n$6\r\n621362\r\n";
        var response = lightClientRequest.SendCommand("TIME", 3);
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse.Length, actualValue.Length);
    }


    [Test]
    public void TimeWithReturnErrorTest()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        string expectedResponse = "-ERR wrong number of arguments for 'time' command\r\n";
        var response = lightClientRequest.SendCommand("TIME HELLO");
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    #endregion

    #region SeClientTests

    [Test]
    public void SeSaveTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
        IServer server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");

        DateTime lastSave = server.LastSave();

        // Check no saves present
        Assert.AreEqual(DateTimeOffset.FromUnixTimeSeconds(0).Ticks, lastSave.Ticks);

        // Issue background save
        server.Save(SaveType.BackgroundSave);

        // Wait for save to complete
        while (server.LastSave() == lastSave) Thread.Sleep(10);
    }

    [Test]
    public void SeSaveRecoverTest([Values] bool disableObj)
    {
        server.Dispose();
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: disableObj);
        server.Start();

        using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
        {
            IDatabase db = redis.GetDatabase(0);
            db.StringSet("SeSaveRecoverTestKey", "SeSaveRecoverTestValue");

            // Issue and wait for DB save
            IServer server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
            server.Save(SaveType.BackgroundSave);
            while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
        }

        server.Dispose(false);
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true);
        server.Start();

        using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
        {
            IDatabase db = redis.GetDatabase(0);
            RedisValue recoveredValue = db.StringGet("SeSaveRecoverTestKey");
            Assert.AreEqual("SeSaveRecoverTestValue", recoveredValue.ToString());
        }
    }

    [Test]
    public void SeSaveRecoverObjectTest()
    {
        string key = "SeSaveRecoverTestObjectKey";
        var ldata = new RedisValue[] { "a", "b", "c", "d" };
        RedisValue[] returned_data_before_recovery = default;
        using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
        {
            IDatabase db = redis.GetDatabase(0);
            db.ListLeftPush(key, ldata);
            ldata = ldata.Select(x => x).Reverse().ToArray();
            returned_data_before_recovery = db.ListRange(key);
            Assert.AreEqual(ldata, returned_data_before_recovery);

            // Issue and wait for DB save
            IServer server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
            server.Save(SaveType.BackgroundSave);
            while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
        }

        server.Dispose(false);
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true);
        server.Start();

        using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
        {
            IDatabase db = redis.GetDatabase(0);
            RedisValue[] returnedData = db.ListRange(key);
            Assert.AreEqual(returned_data_before_recovery, returnedData);
            Assert.AreEqual(ldata.Length, returnedData.Length);
            Assert.AreEqual(ldata, returnedData);
        }
    }
    [Test]
    [TestCase(63, 15, 1)]
    [TestCase(63, 1, 1)]
    [TestCase(16, 16, 1)]
    [TestCase(5, 64, 1)]
    public void SeSaveRecoverMultipleObjectsTest(int memorySize, int recoveryMemorySize, int pageSize)
    {
        string sizeToString(int size) => size + "k";

        server.Dispose();
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, MemorySize: sizeToString(memorySize), PageSize: sizeToString(pageSize));
        server.Start();

        var ldata = new RedisValue[] { "a", "b", "c", "d" };
        RedisValue[] ldataArr = ldata.Select(x => x).Reverse().ToArray();
        using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
        {
            IDatabase db = redis.GetDatabase(0);
            for (int i = 0; i < 3000; i++)
            {
                string key = $"SeSaveRecoverTestKey{i:0000}";
                db.ListLeftPush(key, ldata);
                RedisValue[] retval = db.ListRange(key);
                Assert.AreEqual(ldataArr, retval, $"key {key}");
            }

            // Issue and wait for DB save
            IServer server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
            server.Save(SaveType.BackgroundSave);
            while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
        }

        server.Dispose(false);
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, lowMemory: true, MemorySize: sizeToString(recoveryMemorySize), PageSize: sizeToString(pageSize));
        server.Start();

        Assert.LessOrEqual(server.Provider.StoreWrapper.objectStore.MaxAllocatedPageCount, (recoveryMemorySize / pageSize) + 1);
        using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
        {
            IDatabase db = redis.GetDatabase(0);
            for (int i = 0; i < 3000; i++)
            {
                string key = $"SeSaveRecoverTestKey{i:0000}";
                RedisValue[] returnedData = db.ListRange(key);
                Assert.AreEqual(ldataArr, returnedData, $"key {key}");
            }
        }
    }

    [Test]
    [TestCase("63k", "15k")]
    [TestCase("63k", "3k")]
    [TestCase("63k", "1k")]
    [TestCase("8k", "5k")]
    [TestCase("16k", "16k")]
    [TestCase("5k", "8k")]
    [TestCase("5k", "64k")]
    public void SeSaveRecoverMultipleKeysTest(string memorySize, string recoveryMemorySize)
    {
        bool disableObj = true;

        server.Dispose();
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: disableObj, lowMemory: true, MemorySize: memorySize, PageSize: "1k", enableAOF: true);
        server.Start();

        using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
        {
            IDatabase db = redis.GetDatabase(0);
            for (int i = 0; i < 1000; i++)
            {
                db.StringSet($"SeSaveRecoverTestKey{i:0000}", $"SeSaveRecoverTestValue");
            }

            for (int i = 0; i < 1000; i++)
            {
                RedisValue recoveredValue = db.StringGet($"SeSaveRecoverTestKey{i:0000}");
                Assert.AreEqual("SeSaveRecoverTestValue", recoveredValue.ToString());
            }

            RedisResult inforesult = db.Execute("INFO");

            // Issue and wait for DB save
            IServer server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
            server.Save(SaveType.BackgroundSave);
            while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);

            for (int i = 1000; i < 2000; i++)
            {
                db.StringSet($"SeSaveRecoverTestKey{i:0000}", $"SeSaveRecoverTestValue");
            }

            for (int i = 1000; i < 2000; i++)
            {
                RedisValue recoveredValue = db.StringGet($"SeSaveRecoverTestKey{i:0000}");
                Assert.AreEqual("SeSaveRecoverTestValue", recoveredValue.ToString());
            }

            db.Execute("COMMITAOF");
        }

        server.Dispose(false);
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: disableObj, tryRecover: true, lowMemory: true, MemorySize: recoveryMemorySize, PageSize: "1k", enableAOF: true);
        server.Start();

        using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
        {
            IDatabase db = redis.GetDatabase(0);
            for (int i = 0; i < 2000; i++)
            {
                RedisValue recoveredValue = db.StringGet($"SeSaveRecoverTestKey{i:0000}");
                Assert.AreEqual("SeSaveRecoverTestValue", recoveredValue.ToString(), $"Key SeSaveRecoverTestKey{i:0000}");
            }
        }
    }

    [Test]
    public void SeAofRecoverTest()
    {
        server.Dispose(false);
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
        server.Start();

        using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
        {
            IDatabase db = redis.GetDatabase(0);
            db.StringSet("SeAofRecoverTestKey", "SeAofRecoverTestValue");

            db.Execute("COMMITAOF");
        }

        server.Dispose(false);
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, tryRecover: true);
        server.Start();

        using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
        {
            IDatabase db = redis.GetDatabase(0);
            RedisValue recoveredValue = db.StringGet("SeAofRecoverTestKey");
            Assert.AreEqual("SeAofRecoverTestValue", recoveredValue.ToString());
        }
    }

    [Test]
    public void SeFlushDatabaseTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
        IServer server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");

        IDatabase db = redis.GetDatabase(0);

        string origValue = "abcdefghij";
        db.StringSet("mykey", origValue);

        string retValue = db.StringGet("mykey");
        Assert.AreEqual(origValue, retValue);

        server.FlushDatabase();

        retValue = db.StringGet("mykey");
        Assert.AreEqual(null, retValue);
    }

    [Test]
    public void SePingTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);
        string expectedResponse = "PONG";
        string actualValue = db.Execute("PING").ToString();
        Assert.AreEqual(expectedResponse, actualValue);
    }


    [Test]
    public void SePingMessageTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);
        string expectedResponse = "HELLO";
        string actualValue = db.Execute("PING", "HELLO").ToString();
        Assert.AreEqual(expectedResponse, actualValue);
    }


    [Test]
    public void SePingErrorMessageTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);
        Assert.Throws<RedisServerException>(() => db.Execute("PING", "HELLO", "WORLD"));
    }



    [Test]
    public void SeEchoWithNoMessageReturnErrorTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);
        Assert.Throws<RedisServerException>(() => db.Execute("ECHO"));
    }

    [Test]
    public void SeEchoWithMessageTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);
        string expectedResponse = "HELLO";
        string actualValue = db.Execute("ECHO", "HELLO").ToString();
        Assert.AreEqual(expectedResponse, actualValue);
    }


    [Test]
    public void SeTimeCommandTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);
        RedisResult actualValue = db.Execute("TIME");
        RedisValue seconds = ((RedisValue[])actualValue)[0];
        RedisValue microsecs = ((RedisValue[])actualValue)[1];
        Assert.AreEqual(seconds.ToString().Length, 10);
        Assert.AreEqual(microsecs.ToString().Length, 6);
    }


    [Test]
    public void SeTimeWithReturnErrorTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);
        Assert.Throws<RedisServerException>(() => db.Execute("TIME HELLO").ToString());
    }

    [Test]
    public async Task SeFlushDBTest([Values] bool async, [Values] bool unsafetruncatelog)
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
        IDatabase db = redis.GetDatabase(0);
        string key = "flushdbTest";
        string value = key;

        db.StringSet(key, value);
        RedisValue _value = db.StringGet(key);
        Assert.AreEqual(value, (string)_value);
        string[] p = default;

        if (async && unsafetruncatelog)
            p = ["ASYNC", "UNSAFETRUNCATELOG"];
        else if (unsafetruncatelog)
            p = ["UNSAFETRUNCATELOG"];

        if (async)
        {
            await db.ExecuteAsync("FLUSHDB", p).ConfigureAwait(false);
            _value = db.StringGet(key);
            while (!_value.IsNull)
            {
                _value = db.StringGet(key);
                Thread.Yield();
            }
        }
        else
        {
            db.Execute("FLUSHDB", p);
            _value = db.StringGet(key);
        }
        Assert.IsTrue(_value.IsNull);
    }

    #endregion
}