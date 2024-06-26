﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using StackExchange.Redis;

namespace GarnetClientSample;

/// <summary>
/// Use Garnet with StackExchange.Redis as client library
/// </summary>
public class SERedisSamples
{
    private readonly string address;
    private readonly int port;

    public SERedisSamples(string address, int port)
    {
        this.address = address;
        this.port = port;
    }

    public async Task RunAll()
    {
        await RespPingAsync();
        RespPing();
        SingleSetRename();
        SingleSetGet();
        SingleIncr();
        SingleIncrBy(99);
        SingleDecrBy(99);
        SingleDecr("test", 5);
        SingleIncrNoKey();
        SingleExists();
        SingleDelete();
    }

    private async Task RespPingAsync()
    {
        using ConnectionMultiplexer redis = await ConnectionMultiplexer.ConnectAsync($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);
        await db.PingAsync();
        Console.WriteLine("RespPing: Success");
    }

    private void RespPing()
    {
        using var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);
        db.Ping();
        string cname = redis.ClientName;
        Console.WriteLine("RespPing: Success");
    }

    private void SingleSetRename()
    {
        using var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);

        string origValue = "test1";
        db.StringSet("key1", origValue);

        db.KeyRename("key1", "key2");
        string retValue = db.StringGet("key2");

        if (origValue != retValue)
            Console.WriteLine("SingleSetRename: Error");
        else
            Console.WriteLine("SingleSetRename: Success");
    }

    private void SingleSetGet()
    {
        using var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);

        string origValue = "abcdefg";
        db.StringSet("mykey", origValue);

        string retValue = db.StringGet("mykey");

        if (origValue != retValue)
            Console.WriteLine("SingleSetGet: Error");
        else
            Console.WriteLine("SingleSetGet: Success");
    }

    private void SingleIncr()
    {
        var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);

        // Key storing integer
        int nVal = -100000;
        string strKey = "key1";
        db.StringSet(strKey, nVal);

        // string retValue = db.StringGet("key1");

        db.StringIncrement(strKey);
        int nRetVal = Convert.ToInt32(db.StringGet(strKey));
        if (nVal + 1 != nRetVal)
            Console.WriteLine("SingleIncr: Error");
        else
            Console.WriteLine("SingleIncr: Success");
    }

    private void SingleIncrBy(long nIncr)
    {
        var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);

        // Key storing integer
        int nVal = 1000;

        string strKey = "key1";
        db.StringSet(strKey, nVal);
        RedisValue s = db.StringGet(strKey);

        RedisValue get = db.StringGet(strKey);
        long n = db.StringIncrement(strKey, nIncr);

        int nRetVal = Convert.ToInt32(db.StringGet(strKey));
        if (n != nRetVal)
            Console.WriteLine("SingleIncrBy: Error");
        else
            Console.WriteLine("SingleIncrBy: Success");
    }

    private void SingleDecrBy(long nDecr)
    {
        var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);

        // Key storing integer
        int nVal = 900;

        string strKey = "key1";
        db.StringSet(strKey, nVal);
        RedisValue s = db.StringGet(strKey);

        long n = db.StringDecrement(strKey, nDecr);
        int nRetVal = Convert.ToInt32(db.StringGet(strKey));
        if (nVal - nDecr != nRetVal)
            Console.WriteLine("SingleDecrBy: Error");
        else
            Console.WriteLine("SingleDecrBy: Success");
    }

    private void SingleDecr(string strKey, int nVal)
    {
        var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);

        // Key storing integer
        db.StringSet(strKey, nVal);
        db.StringDecrement(strKey);
        int nRetVal = Convert.ToInt32(db.StringGet(strKey));
        if (nVal - 1 != nRetVal)
            Console.WriteLine("SingleDecr: Error");
        else
            Console.WriteLine("SingleDecr: Success");
    }

    private void SingleIncrNoKey()
    {
        var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);

        // Key storing integer
        string strKey = "key1";
        int init = Convert.ToInt32(db.StringGet(strKey));
        db.StringIncrement(strKey);

        int retVal = Convert.ToInt32(db.StringGet(strKey));

        db.StringIncrement(strKey);
        retVal = Convert.ToInt32(db.StringGet(strKey));

        if (init + 2 != retVal)
            Console.WriteLine("SingleIncrNoKey: Error");
        else
            Console.WriteLine("SingleIncrNoKey: Success");
    }

    private void SingleExists()
    {
        var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);

        // Key storing integer
        int nVal = 100;
        string strKey = "key1";
        db.StringSet(strKey, nVal);

        bool fExists = db.KeyExists("key1", CommandFlags.None);
        if (fExists)
            Console.WriteLine("SingleExists: Success");
        else
            Console.WriteLine("SingleExists: Error");
    }

    private void SingleDelete()
    {
        var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
        IDatabase db = redis.GetDatabase(0);

        // Key storing integer
        int nVal = 100;
        string strKey = "key1";
        db.StringSet(strKey, nVal);
        db.KeyDelete(strKey);

        bool fExists = db.KeyExists("key1", CommandFlags.None);
        if (!fExists)
            Console.WriteLine("Pass: strKey, Key does not exists");
        else
            Console.WriteLine("Fail: strKey, Key was not deleted");
    }
}