﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.Server;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.Tests;

[TestFixture]
internal class RespListTests
{
    private GarnetServer server;
    private Random r;

    [SetUp]
    public void Setup()
    {
        r = new Random(674386);
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
        server.Start();
    }

    [TearDown]
    public void TearDown()
    {
        server.Dispose();
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
    }

    [Test]
    public void BasicLPUSHAndLPOP()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";
        string val = "Value-0";

        int nVals = 1;
        //a entry added to the list
        long nAdded = db.ListLeftPush(key, val);
        Assert.AreEqual(nVals, nAdded);

        RedisResult result = db.Execute("MEMORY", "USAGE", key);
        int actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        int expectedResponse = 184;
        Assert.AreEqual(expectedResponse, actualValue);

        string popval = db.ListLeftPop(key);
        Assert.AreEqual(val, popval);

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 104;
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void MultiLPUSHAndLTRIM()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";
        int nVals = 10;
        RedisValue[] values = new RedisValue[nVals];
        for (int i = 0; i < 10; i++)
        {
            values[i] = "val_" + i.ToString();
        }
        long nAdded = db.ListLeftPush(key, values);
        Assert.AreEqual(nVals, nAdded);

        RedisResult result = db.Execute("MEMORY", "USAGE", key);
        int actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        int expectedResponse = 904;
        Assert.AreEqual(expectedResponse, actualValue);

        long nLen = db.ListLength(key);
        db.ListTrim(key, 1, 5);

        long nLen1 = db.ListLength(key);
        Assert.AreEqual(nLen1, 5);

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 504;
        Assert.AreEqual(expectedResponse, actualValue);

        //all elements remain
        db.ListTrim(key, 0, -1);
        nLen1 = db.ListLength(key);
        Assert.AreEqual(nLen1, 5);

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 504;
        Assert.AreEqual(expectedResponse, actualValue);

        db.ListTrim(key, 0, -3);
        nLen1 = db.ListLength(key);
        Assert.AreEqual(3, nLen1);

        RedisValue[] vals = db.ListRange(key, 0, -1);
        Assert.AreEqual("val_8", vals[0].ToString());
        Assert.AreEqual("val_7", vals[1].ToString());
        Assert.AreEqual("val_6", vals[2].ToString());
    }

    [Test]
    public void MultiLPUSHAndLLENWithPendingStatus()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        int nVals = 100;
        RedisValue[] values = new RedisValue[nVals];
        for (int i = 0; i < 100; i++)
        {
            values[i] = $"val-{i + 1}";
        }

        for (int j = 0; j < 25; j++)
        {
            long nAdded = db.ListLeftPush($"List_Test-{j + 1}", values);
            Assert.AreEqual(nVals, nAdded);
        }

        long nLen = db.ListLength("List_Test-10");
        Assert.AreEqual(100, nLen);
    }

    [Test]
    public void BasicLPUSHAndLTRIM()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";
        string val = "Value-0";

        int nVals = 1;
        //a entry added to the list
        long nAdded = db.ListLeftPush(key, val);
        Assert.AreEqual(nVals, nAdded);

        long nLen = db.ListLength(key);
        db.ListTrim(key, 0, 5);

        long nLen1 = db.ListLength(key);
        Assert.AreEqual(nLen1, 1);

        db.ListTrim(key, 0, -1);
        nLen1 = db.ListLength(key);
        Assert.AreEqual(nLen1, 1);
    }

    [Test]
    public void BasicLPUSHAndLRANGE()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";
        int nVals = 3;
        RedisValue[] values = new RedisValue[nVals];
        for (int i = 0; i < nVals; i++)
        {
            values[i] = "val_" + i.ToString();
        }
        long nAdded = db.ListLeftPush(key, values);
        Assert.AreEqual(nVals, nAdded);

        long nLen = db.ListLength(key);
        RedisValue[] vals = db.ListRange(key, 0, 0);
        Assert.AreEqual(1, vals.Length);

        vals = null;
        vals = db.ListRange(key, -3, 2);
        Assert.AreEqual(3, vals.Length);

        vals = null;
        vals = db.ListRange(key, -100, 100);
        Assert.AreEqual(3, vals.Length);

        vals = null;
        vals = db.ListRange(key, 5, 10);
        Assert.AreEqual(0, vals.Length);
    }

    [Test]
    public void BasicRPUSHAndLINDEX()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";
        int nVals = 3;
        RedisValue[] values = new RedisValue[nVals];
        for (int i = 0; i < nVals; i++)
        {
            values[i] = "val_" + i.ToString();
        }
        long nAdded = db.ListRightPush(key, values);
        Assert.AreEqual(nVals, nAdded);

        long nLen = db.ListLength(key);
        int nIndex = -1;
        RedisValue val = db.ListGetByIndex(key, nIndex);
        Assert.AreEqual(val, values[nVals + nIndex]);

        nIndex = 2;
        val = db.ListGetByIndex(key, nIndex);
        Assert.AreEqual(val, values[nIndex]);

        nIndex = 3;
        val = db.ListGetByIndex(key, nIndex);
        Assert.AreEqual(true, val.IsNull);
    }

    [Test]
    public void BasicRPUSHAndLINSERT()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";
        int nVals = 3;
        RedisValue[] values = new RedisValue[nVals];
        for (int i = 0; i < nVals; i++)
        {
            values[i] = "val_" + i.ToString();
        }
        long nAdded = db.ListRightPush(key, values);
        Assert.AreEqual(nVals, nAdded);

        RedisResult result = db.Execute("MEMORY", "USAGE", key);
        int actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        int expectedResponse = 344;
        Assert.AreEqual(expectedResponse, actualValue);

        long nLen = db.ListLength(key);
        string insert_val = "val_test1";
        // test before
        long ret = db.ListInsertBefore(key, "val_1", insert_val);

        nLen = db.ListLength(key);
        RedisValue val = db.ListGetByIndex(key, 1);
        Assert.AreEqual(val, insert_val);

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 432;
        Assert.AreEqual(expectedResponse, actualValue);

        // test after
        insert_val = "val_test2";
        db.ListInsertAfter(key, "val_0", insert_val);
        val = db.ListGetByIndex(key, 1);
        Assert.AreEqual(val, insert_val);

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 520;
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void BasicRPUSHAndLREM()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";
        int nVals = 6;
        RedisValue[] values = new RedisValue[nVals];
        for (int i = 0; i < nVals; i += 2)
        {
            values[i] = "val_" + i.ToString();
            values[i + 1] = "val_" + i.ToString();

        }
        long nAdded = db.ListRightPush(key, values);
        Assert.AreEqual(nVals, nAdded);

        RedisResult result = db.Execute("MEMORY", "USAGE", key);
        int actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        int expectedResponse = 584;
        Assert.AreEqual(expectedResponse, actualValue);

        long nLen = db.ListLength(key);
        long ret = db.ListRemove(key, "val_0", 2);
        Assert.AreEqual(ret, 2);

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 424;
        Assert.AreEqual(expectedResponse, actualValue);

        ret = db.ListRemove(key, "val_4", -1);
        nLen = db.ListLength(key);
        Assert.AreEqual(nLen, 3);

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 344;
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void MultiLPUSHAndLPOPV1()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";

        int nVals = 10;
        RedisValue[] values = new RedisValue[nVals];
        for (int i = 0; i < 10; i++)
        {
            values[i] = "val_" + i.ToString();
        }
        long nAdded = db.ListLeftPush(key, values);
        Assert.AreEqual(nVals, nAdded);

        RedisResult result = db.Execute("MEMORY", "USAGE", key);
        int actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        int expectedResponse = 904;
        Assert.AreEqual(expectedResponse, actualValue);

        long nLen = db.ListLength(key);

        string popval = string.Empty;
        while (true)
        {
            popval = db.ListLeftPop(key);
            Assert.AreEqual(values[nVals - 1], popval);
            nVals--;

            if (nVals == 0)
                break;
        }

        // list is empty, the code should return (nil)
        popval = db.ListLeftPop(key);
        Assert.AreEqual(null, popval);

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 104;
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void MultiLPUSHAndLPOPV2()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";

        int nVals = 10;
        RedisValue[] values = new RedisValue[nVals];
        for (int i = 0; i < 10; i++)
        {
            values[i] = "val_" + i.ToString();
        }
        long nAdded = db.ListLeftPush(key, values);
        Assert.AreEqual(nVals, nAdded);

        RedisResult result = db.Execute("MEMORY", "USAGE", key);
        int actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        int expectedResponse = 904;
        Assert.AreEqual(expectedResponse, actualValue);

        long nLen = db.ListLength(key);
        RedisResult ret = db.Execute("LPOP", key, "2");
        nLen = db.ListLength(key);
        Assert.AreEqual(nLen, 8);

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 744;
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void MultiRPUSHAndRPOP()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";

        int nVals = 10;
        RedisValue[] values = new RedisValue[nVals];
        for (int i = 0; i < 10; i++)
        {
            values[i] = "val_" + i.ToString();
        }
        long nAdded = db.ListRightPush(key, values);
        Assert.AreEqual(nVals, nAdded);

        RedisResult result = db.Execute("MEMORY", "USAGE", key);
        int actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        int expectedResponse = 904;
        Assert.AreEqual(expectedResponse, actualValue);

        string popval = string.Empty;
        nVals = 9;
        while (true)
        {
            popval = db.ListRightPop(key);
            Assert.AreEqual(values[nVals], popval);
            nVals--;

            if (nVals < 0)
                break;
        }

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 104;
        Assert.AreEqual(expectedResponse, actualValue);

        // list is empty, the code should return (nil)
        popval = db.ListLeftPop(key);
        Assert.AreEqual(null, popval);

        result = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == result.Type ? int.Parse(result.ToString()) : -1;
        expectedResponse = 104;
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void BasicRPUSHAndRPOP()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "List_Test";

        int nVals = 1;
        //a entry added to the list
        long nAdded = db.ListRightPush(key, "Value-0");
        Assert.AreEqual(nVals, nAdded);
    }

    [Test]
    public void CanDoRPopLPush()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "mylist";
        db.ListRightPush(key, "Value-one");
        db.ListRightPush(key, "Value-two");
        db.ListRightPush(key, "Value-three");

        RedisValue result = db.ListRightPopLeftPush("mylist", "myotherlist");
        Assert.AreEqual("Value-three", result.ToString());

        RedisResult response = db.Execute("MEMORY", "USAGE", key);
        int actualValue = ResultType.Integer == response.Type ? int.Parse(response.ToString()) : -1;
        int expectedResponse = 272;
        Assert.AreEqual(expectedResponse, actualValue);

        RedisValue[] lrange = db.ListRange(key, 0, -1);
        Assert.AreEqual(2, lrange.Length);
        Assert.AreEqual("Value-one", lrange[0].ToString());
        Assert.AreEqual("Value-two", lrange[1].ToString());

        lrange = db.ListRange("myotherlist", 0, -1);
        Assert.AreEqual(1, lrange.Length);
        Assert.AreEqual("Value-three", lrange[0].ToString());

        result = db.ListRightPopLeftPush(key, key);
        Assert.AreEqual("Value-two", result.ToString());

        response = db.Execute("MEMORY", "USAGE", key);
        actualValue = ResultType.Integer == response.Type ? int.Parse(response.ToString()) : -1;
        expectedResponse = 272;
        Assert.AreEqual(expectedResponse, actualValue);

        lrange = db.ListRange(key, 0, -1);
        Assert.AreEqual(2, lrange.Length);
        Assert.AreEqual("Value-two", lrange[0].ToString());
        Assert.AreEqual("Value-one", lrange[1].ToString());
    }

    [Test]
    public void CanDoLRANGEbasic()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "mylist";
        _ = db.ListRightPush(key, "one");
        _ = db.ListRightPush(key, "two");
        _ = db.ListRightPush(key, "three");

        RedisValue[] result = db.ListRange(key, 0, 0);
        Assert.AreEqual(1, result.Length);
        Assert.IsTrue(Array.Exists(result, t => t.ToString().Equals("one")));

        result = db.ListRange(key, -3, 2);
        Assert.AreEqual(3, result.Length);
        Assert.IsTrue(Array.Exists(result, t => t.ToString().Equals("one")));
        Assert.IsTrue(Array.Exists(result, t => t.ToString().Equals("two")));
        Assert.IsTrue(Array.Exists(result, t => t.ToString().Equals("three")));

        result = db.ListRange(key, -100, 100);
        Assert.AreEqual(3, result.Length);
        Assert.IsTrue(Array.Exists(result, t => t.ToString().Equals("one")));
        Assert.IsTrue(Array.Exists(result, t => t.ToString().Equals("two")));
        Assert.IsTrue(Array.Exists(result, t => t.ToString().Equals("three")));

        result = db.ListRange(key, 5, 100);
        Assert.AreEqual(0, result.Length);
    }

    [Test]
    public void CanDoLRANGEcorrect()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "mylist";
        _ = db.ListRightPush(key, "a");
        _ = db.ListRightPush(key, "b");
        _ = db.ListRightPush(key, "c");
        _ = db.ListRightPush(key, "d");
        _ = db.ListRightPush(key, "e");
        _ = db.ListRightPush(key, "f");
        _ = db.ListRightPush(key, "g");

        RedisValue[] result = db.ListRange(key, -10, -7);
        Assert.AreEqual(1, result.Length);
        Assert.IsTrue(result[0].ToString().Equals("a"));

        result = db.ListRange(key, -4, -2);
        Assert.AreEqual(3, result.Length);
        Assert.IsTrue(result[0].ToString().Equals("d"));
        Assert.IsTrue(result[1].ToString().Equals("e"));
        Assert.IsTrue(result[2].ToString().Equals("f"));

        result = db.ListRange(key, -1, -1);
        Assert.AreEqual(1, result.Length);
        Assert.IsTrue(result[0].ToString().Equals("g"));

        result = db.ListRange(key, -3, 3);
        Assert.AreEqual(0, result.Length);

        result = db.ListRange(key, -3, 4);
        Assert.AreEqual(1, result.Length);
        Assert.IsTrue(result[0].ToString().Equals("e"));

        result = db.ListRange(key, -4, 4);
        Assert.AreEqual(2, result.Length);
        Assert.IsTrue(result[0].ToString().Equals("d"));
        Assert.IsTrue(result[1].ToString().Equals("e"));

        result = db.ListRange(key, 0, 0);
        Assert.AreEqual(1, result.Length);
        Assert.IsTrue(result[0].ToString().Equals("a"));

        result = db.ListRange(key, 1, 2);
        Assert.AreEqual(2, result.Length);
        Assert.IsTrue(result[0].ToString().Equals("b"));
        Assert.IsTrue(result[1].ToString().Equals("c"));

        result = db.ListRange(key, 3, 3);
        Assert.AreEqual(1, result.Length);
        Assert.IsTrue(result[0].ToString().Equals("d"));

        result = db.ListRange(key, 4, 4);
        Assert.AreEqual(1, result.Length);
        Assert.IsTrue(result[0].ToString().Equals("e"));

        result = db.ListRange(key, 5, 10);
        Assert.AreEqual(2, result.Length);
        Assert.IsTrue(result[0].ToString().Equals("f"));
        Assert.IsTrue(result[1].ToString().Equals("g"));
    }

    [Test]
    public void CanDoLSETbasic()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        string key = "mylist";
        var values = new RedisValue[] { "one", "two", "three" };
        long pushResult = db.ListRightPush(key, values);
        Assert.AreEqual(3, pushResult);

        db.ListSetByIndex(key, 0, "four");
        db.ListSetByIndex(key, -2, "five");

        RedisValue[] result = db.ListRange(key, 0, -1);
        string[] strResult = result.Select(r => r.ToString()).ToArray();
        Assert.AreEqual(3, result.Length);
        string[] expected = new[] { "four", "five", "three" };
        Assert.IsTrue(expected.SequenceEqual(strResult));
    }

    #region GarnetClientTests

    [Test]
    public async Task CanDoRPopLPushGC()
    {
        using var db = TestUtils.GetGarnetClient();
        db.Connect();

        //If source does not exist, the value nil is returned and no operation is performed.
        var response = await db.ExecuteForStringResultAsync("RPOPLPUSH", ["mylist", "myotherlist"]);
        Assert.AreEqual(null, response);

        await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "one"]);
        await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "two"]);
        await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "three"]);

        response = await db.ExecuteForStringResultAsync("RPOPLPUSH", ["mylist", "myotherlist"]);
        Assert.AreEqual("three", response);

        var responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
        string[] expectedResponseArray = new string[] { "one", "two" };
        Assert.AreEqual(expectedResponseArray, responseArray);

        responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["myotherlist", "0", "-1"]);
        expectedResponseArray = ["three"];
        Assert.AreEqual(expectedResponseArray, responseArray);

        // if source and destination are the same 
        //the operation is equivalent to removing the last element from the list and pushing it as first element of the list,
        //so it can be considered as a list rotation command.
        response = await db.ExecuteForStringResultAsync("RPOPLPUSH", ["mylist", "mylist"]);
        Assert.AreEqual("two", response);

        responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
        expectedResponseArray = ["two", "one"];
        Assert.AreEqual(expectedResponseArray, responseArray);
    }


    [Test]
    public async Task CanUseLMoveGC()
    {
        using var db = TestUtils.GetGarnetClient();
        db.Connect();

        // Test for Operation direction error.
        Exception exception = Assert.ThrowsAsync<Exception>(async () =>
        {
            await db.ExecuteForStringResultAsync("LMOVE", new string[] { "mylist", "myotherlist", "right", "lef" });
        });
        Assert.AreEqual("ERR syntax error", exception.Message);

        //If source does not exist, the value nil is returned and no operation is performed.
        var response = await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "myotherlist", "RIGHT", "LEFT"]);
        Assert.AreEqual(null, response);

        await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "one"]);
        await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "two"]);
        await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "three"]);

        response = await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "myotherlist", "RIGHT", "LEFT"]);
        Assert.AreEqual("three", response);

        var responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
        string[] expectedResponseArray = new string[] { "one", "two" };
        Assert.AreEqual(expectedResponseArray, responseArray);

        responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["myotherlist", "0", "-1"]);
        expectedResponseArray = ["three"];
        Assert.AreEqual(expectedResponseArray, responseArray);

        response = await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "myotherlist", "LEFT", "RIGHT"]);
        Assert.AreEqual("one", response);

        responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
        expectedResponseArray = ["two"];
        Assert.AreEqual(expectedResponseArray, responseArray);

        responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["myotherlist", "0", "-1"]);
        expectedResponseArray = ["three", "one"];
        Assert.AreEqual(expectedResponseArray, responseArray);

        // if source and destination are the same 
        //the operation is equivalent to a list rotation command.
        response = await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "mylist", "LEFT", "RIGHT"]);
        Assert.AreEqual("two", response);

        response = await db.ExecuteForStringResultAsync("LMOVE", ["myotherlist", "myotherlist", "LEFT", "RIGHT"]);
        Assert.AreEqual("three", response);

        responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["myotherlist", "0", "-1"]);
        expectedResponseArray = ["one", "three"];
        Assert.AreEqual(expectedResponseArray, responseArray);
    }

    [Test]
    public async Task CanUseLMoveWithCaseInsensitiveDirectionGC()
    {
        using var db = TestUtils.GetGarnetClient();
        db.Connect();

        await db.ExecuteForStringResultAsync("RPUSH", new string[] { "mylist", "one" });
        await db.ExecuteForStringResultAsync("RPUSH", new string[] { "mylist", "two" });
        await db.ExecuteForStringResultAsync("RPUSH", new string[] { "mylist", "three" });

        var response = await db.ExecuteForStringResultAsync("LMOVE", new string[] { "mylist", "myotherlist", "right", "left" });
        Assert.AreEqual("three", response);

        var responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", new string[] { "mylist", "0", "-1" });
        string[] expectedResponseArray = new string[] { "one", "two" };
        Assert.AreEqual(expectedResponseArray, responseArray);

        responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", new string[] { "myotherlist", "0", "-1" });
        expectedResponseArray = new string[] { "three" };
        Assert.AreEqual(expectedResponseArray, responseArray);

        response = await db.ExecuteForStringResultAsync("LMOVE", new string[] { "mylist", "myotherlist", "LeFT", "RIghT" });
        Assert.AreEqual("one", response);

        responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", new string[] { "mylist", "0", "-1" });
        expectedResponseArray = new string[] { "two" };
        Assert.AreEqual(expectedResponseArray, responseArray);

        responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", new string[] { "myotherlist", "0", "-1" });
        expectedResponseArray = new string[] { "three", "one" };
        Assert.AreEqual(expectedResponseArray, responseArray);
    }

    [Test]
    public async Task CanUseLMoveWithCancellationTokenGC()
    {
        using var db = TestUtils.GetGarnetClient();
        db.Connect();

        await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "one"]);
        await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "two"]);
        await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "three"]);

        var tokenSource = new CancellationTokenSource();
        CancellationToken token = tokenSource.Token;
        var response = await db.ExecuteForStringResultWithCancellationAsync("LMOVE", ["mylist", "myotherlist", "RIGHT", "LEFT"], token);
        Assert.AreEqual("three", response);

        //check contents of mylist sorted set
        var responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
        string[] expectedResponseArray = new string[] { "one", "two" };
        Assert.AreEqual(expectedResponseArray, responseArray);

        //Assert the cancellation is seen
        tokenSource.Cancel();
        var t = db.ExecuteForStringResultWithCancellationAsync("LMOVE", ["myotherlist", "myotherlist", "LEFT", "RIGHT"], tokenSource.Token);
        Assert.Throws<OperationCanceledException>(() => t.Wait(tokenSource.Token));

        tokenSource.Dispose();
    }
    #endregion

    #region LightClientTests

    [Test]
    public void CanReturnEmptyArrayinListLC()
    {
        using var lightClientRequest = TestUtils.CreateRequest();

        lightClientRequest.SendCommands("RPUSH mylist Hello", "PING", 1, 1);
        lightClientRequest.SendCommands("RPUSH mylist foo", "PING", 1, 1);
        lightClientRequest.SendCommands("RPUSH mylist bar", "PING", 1, 1);

        var response = lightClientRequest.SendCommands("LRANGE mylist 0 -1", "PING", 4, 1);
        string expectedResponse = "*3\r\n$5\r\nHello\r\n$3\r\nfoo\r\n$3\r\nbar\r\n+PONG\r\n";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);

        response = lightClientRequest.SendCommands("LRANGE mylist 5 10", "PING", 1, 1);
        expectedResponse = "*0\r\n+PONG\r\n";
        actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanReturnNilWhenNonExistingListLC()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        var response = lightClientRequest.SendCommands("LINSERT mykey BEFORE \"hola\" \"bye\"", "PING", 1, 1);
        //0 if key does not exist
        string expectedResponse = ":0\r\n+PONG\r\n";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanReturnErrorWhenMissingParametersLC()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        var response = lightClientRequest.SendCommands("LINSERT mykey", "PING", 1, 1);
        string expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, "LINSERT")}\r\n+PONG\r\n";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanDoPushAndTrimLC()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        lightClientRequest.SendCommand("RPUSH mylist one");
        lightClientRequest.SendCommand("RPUSH mylist two");
        var response = lightClientRequest.SendCommand("RPUSH mylist three");
        string expectedResponse = ":3\r\n";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);

        response = lightClientRequest.SendCommand("LTRIM mylist 1 -1");
        expectedResponse = "+OK\r\n";
        actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);

        response = lightClientRequest.SendCommand("LRANGE mylist 0 -1", 3);
        expectedResponse = "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n";
        actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanDoLInsertBeforeAndAfterLC()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        lightClientRequest.SendCommand("RPUSH mylist Hello");
        lightClientRequest.SendCommand("RPUSH mylist World");
        // Use Before
        var response = lightClientRequest.SendCommand("LINSERT mylist BEFORE World There");
        string expectedResponse = ":3\r\n";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);

        response = lightClientRequest.SendCommand("LRANGE mylist 0 -1", 4);
        expectedResponse = "*3\r\n$5\r\nHello\r\n$5\r\nThere\r\n$5\r\nWorld\r\n";
        actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);

        // Use After
        response = lightClientRequest.SendCommand("LINSERT mylist AFTER World Bye");
        expectedResponse = ":4\r\n";
        actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);

        response = lightClientRequest.SendCommand("LRANGE mylist 0 -1", 5);
        expectedResponse = "*4\r\n$5\r\nHello\r\n$5\r\nThere\r\n$5\r\nWorld\r\n$3\r\nBye\r\n";
        actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanDoLInsertWithNoElementLC()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        lightClientRequest.SendCommand("RPUSH mylist Hello");
        lightClientRequest.SendCommand("RPUSH mylist World");
        // Use Before
        var response = lightClientRequest.SendCommand("LINSERT mylist BEFORE There Today");
        string expectedResponse = ":-1\r\n";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanSendErrorInWrongTypeLC()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        var response = lightClientRequest.SendCommand("HSET myhash onekey onepair");
        lightClientRequest.SendCommand("LINSERT myhash BEFORE one two");
        string expectedResponse = "-ERR wrong key type used in LINSERT command.\r\n";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanDoLSETbasicLC()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        _ = lightClientRequest.SendCommand("RPUSH mylist one two three");
        var response = lightClientRequest.SendCommand("LSET mylist 0 four");
        string expectedResponse = "+OK\r\n";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanReturnErrorLSETWhenNosuchkey()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        var response = lightClientRequest.SendCommand("LSET mylist 0 four");
        string expectedResponse = "-ERR no such key\r\n";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanReturnErrorLSETWhenIndexNotInteger()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        _ = lightClientRequest.SendCommand("RPUSH mylist one two three");
        var response = lightClientRequest.SendCommand("LSET mylist a four");
        string expectedResponse = "-ERR value is not an integer or out of range.\r\n";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanReturnErrorLSETWhenIndexOutRange()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        _ = lightClientRequest.SendCommand("RPUSH mylist one two three");
        var response = lightClientRequest.SendCommand("LSET mylist 10 four");
        // 
        string expectedResponse = "-ERR index out of range";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);

        response = lightClientRequest.SendCommand("LSET mylist -100 four");
        expectedResponse = "-ERR index out of range";
        actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    [Test]
    public void CanReturnErrorLSETWhenArgumentsWrong()
    {
        using var lightClientRequest = TestUtils.CreateRequest();
        _ = lightClientRequest.SendCommand("RPUSH mylist one two three");
        var response = lightClientRequest.SendCommand("LSET mylist a");
        string expectedResponse = "-ERR wrong number of arguments for 'LSET'";
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }

    #endregion

    [Test]
    public void CanHandleNoPrexistentKey()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        int iter = 100;
        string key = "lkey";

        for (int i = 0; i < iter; i++)
        {
            //LLEN
            long count = db.ListLength(key);
            Assert.AreEqual(0, count);
            count = db.ListLength(key);
            Assert.AreEqual(0, count);
            Assert.IsFalse(db.KeyExists(key));

            //LPOP
            RedisValue result = db.ListLeftPop(key);
            Assert.IsTrue(result.IsNull);
            result = db.ListLeftPop(key);
            Assert.IsTrue(result.IsNull);
            Assert.IsFalse(db.KeyExists(key));

            //RPOP
            result = db.ListRightPop(key);
            Assert.IsTrue(result.IsNull);
            result = db.ListRightPop(key);
            Assert.IsTrue(result.IsNull);
            Assert.IsFalse(db.KeyExists(key));

            //LPOP count
            RedisValue[] resultArray = db.ListLeftPop(key, 100);
            Assert.AreEqual(Array.Empty<RedisValue>(), resultArray);
            resultArray = db.ListLeftPop(key, 100);
            Assert.AreEqual(Array.Empty<RedisValue>(), resultArray);
            Assert.IsFalse(db.KeyExists(key));

            //RPOP count
            resultArray = db.ListRightPop(key, 100);
            Assert.AreEqual(Array.Empty<RedisValue>(), resultArray);
            resultArray = db.ListRightPop(key, 100);
            Assert.AreEqual(Array.Empty<RedisValue>(), resultArray);
            Assert.IsFalse(db.KeyExists(key));

            //LRANGE
            resultArray = db.ListRange(key);
            Assert.AreEqual(Array.Empty<RedisValue>(), resultArray);
            resultArray = db.ListRange(key);
            Assert.AreEqual(Array.Empty<RedisValue>(), resultArray);
            Assert.IsFalse(db.KeyExists(key));

            //LINDEX
            result = db.ListGetByIndex(key, 15);
            Assert.IsTrue(result.IsNull);
            result = db.ListGetByIndex(key, 15);
            Assert.IsTrue(result.IsNull);
            Assert.IsFalse(db.KeyExists(key));

            //LTRIM
            db.ListTrim(key, 0, 15);
            db.ListTrim(key, 0, 15);
            db.ListTrim(key, 0, 15);
            Assert.IsFalse(db.KeyExists(key));

            //LREM
            count = db.ListRemove(key, "hello", 100);
            Assert.AreEqual(0, count);
            count = db.ListRemove(key, "hello", 100);
            Assert.AreEqual(0, count);
            Assert.IsFalse(db.KeyExists(key));
        }
    }

    [Test]
    [Repeat(10)]
    public void ListPushPopStressTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        int keyCount = 10;
        int ppCount = 100;
        //string[] keys = new string[keyCount];
        HashSet<string> keys = [];
        for (int i = 0; i < keyCount; i++)
            while (!keys.Add(r.Next().ToString())) { }

        Assert.AreEqual(keyCount, keys.Count, "Unique key initialization failed!");

        string[] keyArray = keys.ToArray();
        Task[] tasks = new Task[keyArray.Length << 1];
        for (int i = 0; i < tasks.Length; i += 2)
        {
            int idx = i;
            tasks[i] = Task.Run(async () =>
            {
                string key = keyArray[idx >> 1];
                for (int j = 0; j < ppCount; j++)
                    await db.ListLeftPushAsync(key, j);
            });

            tasks[i + 1] = Task.Run(() =>
            {
                string key = keyArray[idx >> 1];
                for (int j = 0; j < ppCount; j++)
                {
                    RedisValue value = db.ListRightPop(key);
                    while (value.IsNull)
                    {
                        Thread.Yield();
                        value = db.ListRightPop(key);
                    }
                    Assert.IsTrue((int)value >= 0 && (int)value < ppCount, "Pop value inconsistency");
                }
            });
        }
        Task.WaitAll(tasks);

        foreach (string key in keyArray)
        {
            long count = db.ListLength(key);
            Assert.AreEqual(0, count);
        }
    }

    [Test]
    [TestCase(10)]
    [TestCase(24)]
    [TestCase(100)]
    public void CanDoLMoveChunks(int bytesPerSend)
    {
        using var lightClientRequest = TestUtils.CreateRequest();

        lightClientRequest.SendCommandChunks("RPUSH mylist value-one", bytesPerSend);
        lightClientRequest.SendCommandChunks("RPUSH mylist value-two", bytesPerSend);
        lightClientRequest.SendCommandChunks("RPUSH mylist value-three", bytesPerSend);

        string expectedResponse = "$11\r\nvalue-three\r\n";
        var response = lightClientRequest.SendCommandChunks("RPOPLPUSH mylist myotherlist", bytesPerSend);
        string actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);

        expectedResponse = "*2\r\n$9\r\nvalue-one\r\n$9\r\nvalue-two\r\n";
        response = lightClientRequest.SendCommandChunks("LRANGE mylist 0 -1", bytesPerSend, 3);
        actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }


    [Test]
    public void CanDoLPopMultipleValues()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        var vals = new RedisValue[10];

        for (int i = 1; i <= vals.Length; i++)
        {
            vals[i - 1] = new RedisValue($"valkey-{i}");
        }
        db.ListLeftPush("mylist", vals);
        db.ListRightPop("mylist", 5);
        Assert.IsTrue(db.ListLength("mylist") == 5);
    }

    [Test]
    public void CanDoLPushXRpushX()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        IDatabase db = redis.GetDatabase(0);

        var vals = new RedisValue[10];

        for (int i = 1; i <= vals.Length; i++)
        {
            vals[i - 1] = new RedisValue($"valkey-{i}");
        }

        //this should not create any list
        long result = db.ListLeftPush("mylist", vals, When.Exists);
        Assert.IsTrue(result == 0);

        RedisResult response = db.Execute("MEMORY", "USAGE", "mylist");
        int actualValue = ResultType.Integer == response.Type ? int.Parse(response.ToString()) : -1;
        int expectedResponse = -1;
        Assert.AreEqual(expectedResponse, actualValue);

        // this should create the list
        result = db.ListLeftPush("mylist", vals);
        Assert.IsTrue(result == 10);

        response = db.Execute("MEMORY", "USAGE", "mylist");
        actualValue = ResultType.Integer == response.Type ? int.Parse(response.ToString()) : -1;
        expectedResponse = 904;
        Assert.AreEqual(expectedResponse, actualValue);

        //this should not create a new list
        result = db.ListRightPush("myaux-list", vals, When.Exists);
        Assert.IsTrue(result == 0);

        response = db.Execute("MEMORY", "USAGE", "myaux-list");
        actualValue = ResultType.Integer == response.Type ? int.Parse(response.ToString()) : -1;
        expectedResponse = -1;
        Assert.AreEqual(expectedResponse, actualValue);

        //this should create the list
        result = db.ListRightPush("myaux-list", vals, When.Always);
        Assert.IsTrue(result == 10);

        response = db.Execute("MEMORY", "USAGE", "myaux-list");
        actualValue = ResultType.Integer == response.Type ? int.Parse(response.ToString()) : -1;
        expectedResponse = 912;
        Assert.AreEqual(expectedResponse, actualValue);
    }


    [Test]
    public void CanDoLPushxRPushx()
    {
        using var lightClientRequest = TestUtils.CreateRequest();

        var response = lightClientRequest.SendCommand("HSET mylist field1 value");

        //this operation should affect only if key already exists and holds a list.
        lightClientRequest.SendCommand("LPUSHX mylist value-two");
        lightClientRequest.SendCommand("RPUSHX mylist value-one");
        var len = lightClientRequest.SendCommand("LLEN mylist");

        string expectedResponse = ":0\r\n";
        string actualValue = Encoding.ASCII.GetString(len).Substring(0, expectedResponse.Length);
        Assert.AreEqual(expectedResponse, actualValue);
    }
}