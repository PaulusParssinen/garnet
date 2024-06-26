﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Server;
using NUnit.Framework;
using Tsavorite;

namespace Garnet.Tests;

[TestFixture]
public class GarnetObjectTests
{
    private TsavoriteKV<byte[], IGarnetObject> store;
    private IDevice logDevice, objectLogDevice;

    [SetUp]
    public void Setup()
    {
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        CreateStore();
    }

    [TearDown]
    public void TearDown()
    {
        store.Dispose();
        logDevice.Dispose();
        objectLogDevice.Dispose();
        logDevice = objectLogDevice = null;
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
    }

    [Test]
    public void WriteRead()
    {
        using ClientSession<byte[], IGarnetObject, IGarnetObject, IGarnetObject, Empty, SimpleFunctions<byte[], IGarnetObject, Empty>> session = store.NewSession<IGarnetObject, IGarnetObject, Empty, SimpleFunctions<byte[], IGarnetObject, Empty>>(new SimpleFunctions<byte[], IGarnetObject, Empty>());

        byte[] key = new byte[] { 0 };
        var obj = new SortedSetObject();

        session.Upsert(key, obj);

        IGarnetObject output = null;
        Status status = session.Read(ref key, ref output);

        Assert.IsTrue(status.Found);
        Assert.AreEqual(obj, output);
    }

    [Test]
    public async Task WriteCheckpointRead()
    {
        ClientSession<byte[], IGarnetObject, IGarnetObject, IGarnetObject, Empty, MyFunctions> session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());

        byte[] key = new byte[] { 0 };
        var obj = new SortedSetObject();
        obj.Add([15], 10);

        session.Upsert(key, obj);

        session.Dispose();

        await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);

        store.Dispose();
        CreateStore();

        store.Recover();

        session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());

        IGarnetObject output = null;
        Status status = session.Read(ref key, ref output);

        session.Dispose();

        Assert.IsTrue(status.Found);
        Assert.IsTrue(obj.Equals((SortedSetObject)output));
    }

    [Test]
    public async Task CopyUpdate()
    {
        ClientSession<byte[], IGarnetObject, IGarnetObject, IGarnetObject, Empty, MyFunctions> session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());

        byte[] key = new byte[] { 0 };
        IGarnetObject obj = new SortedSetObject();
        ((SortedSetObject)obj).Add([15], 10);

        session.Upsert(key, obj);

        store.Log.Flush(true);

        session.RMW(ref key, ref obj);

        session.Dispose();

        await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);

        store.Dispose();
        CreateStore();

        store.Recover();

        session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());

        IGarnetObject output = null;
        Status status = session.Read(ref key, ref output);

        session.Dispose();

        Assert.IsTrue(status.Found);
        Assert.IsTrue(((SortedSetObject)obj).Equals((SortedSetObject)output));
    }

    private class MyFunctions : FunctionsBase<byte[], IGarnetObject, IGarnetObject, IGarnetObject, Empty>
    {
        public MyFunctions()
        { }

        public override bool SingleReader(ref byte[] key, ref IGarnetObject input, ref IGarnetObject value, ref IGarnetObject dst, ref ReadInfo updateInfo)
        {
            dst = value;
            return true;
        }

        public override bool ConcurrentReader(ref byte[] key, ref IGarnetObject input, ref IGarnetObject value, ref IGarnetObject dst, ref ReadInfo updateInfo, ref RecordInfo recordInfo)
        {
            dst = value;
            return true;
        }

        public override bool CopyUpdater(ref byte[] key, ref IGarnetObject input, ref IGarnetObject oldValue, ref IGarnetObject newValue, ref IGarnetObject output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            oldValue.CopyUpdate(ref oldValue, ref newValue, false);
            return true;
        }
    }

    private void CreateStore()
    {
        if (logDevice == null)
            logDevice = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog.log");
        if (objectLogDevice == null)
            objectLogDevice = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog.obj.log");
        var log = new LogSettings
        {
            LogDevice = logDevice,
            ObjectLogDevice = objectLogDevice,
        };

        var ckpt = new CheckpointSettings
        {
            CheckpointDir = TestUtils.MethodTestDir
        };

        var serializer = new SerializerSettings<byte[], IGarnetObject>
        {
            valueSerializer = () => new MyGarnetObjectSerializer()
        };

        store = new TsavoriteKV<byte[], IGarnetObject>(128, log, ckpt, serializer);
    }
}

/// <summary>
/// Serializer for IGarnetObject
/// </summary>
internal sealed class MyGarnetObjectSerializer : BinaryObjectSerializer<IGarnetObject>
{
    /// <inheritdoc />
    public override void Deserialize(out IGarnetObject obj)
    {
        var type = (GarnetObjectType)reader.ReadByte();
        obj = type switch
        {
            GarnetObjectType.SortedSet => new SortedSetObject(reader),
            GarnetObjectType.List => new ListObject(reader),
            GarnetObjectType.Hash => new HashObject(reader),
            GarnetObjectType.Set => new SetObject(reader),
            _ => null,
        };
    }

    /// <inheritdoc />
    public override void Serialize(ref IGarnetObject obj)
    {
        if (obj == null)
            writer.Write((byte)GarnetObjectType.Null);
        else
            obj.Serialize(writer);
    }
}