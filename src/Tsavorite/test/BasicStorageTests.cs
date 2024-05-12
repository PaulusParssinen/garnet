// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.Device;

namespace Tsavorite.Tests;

[TestFixture]
internal class BasicStorageTests
{
    private TsavoriteKV<KeyStruct, ValueStruct> store;

    [Test]
    [Category("TsavoriteKV")]
    public void LocalStorageWriteRead()
    {
        TestDeviceWriteRead(Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "BasicDiskTests.log"), deleteOnClose: true));
    }


    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public void TieredWriteRead()
    {
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        IDevice tested;
        IDevice localDevice = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "BasicDiskTests.log"), deleteOnClose: true, capacity: 1 << 30);
        
        IDevice localDevice2 = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "BasicDiskTests2.log"), deleteOnClose: true, capacity: 1 << 30);
        tested = new TieredStorageDevice(1, localDevice, localDevice2);

        TestDeviceWriteRead(tested);
    }

    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public void ShardedWriteRead()
    {
        IDevice localDevice1 = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "BasicDiskTests1.log"), deleteOnClose: true, capacity: 1 << 30);
        IDevice localDevice2 = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "BasicDiskTests2.log"), deleteOnClose: true, capacity: 1 << 30);
        var device = new ShardedStorageDevice(new UniformPartitionScheme(512, localDevice1, localDevice2));
        TestDeviceWriteRead(device);
    }

    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public void OmitSegmentIdTest([Values] TestUtils.DeviceType deviceType)
    {
        string filename = Path.Join(TestUtils.MethodTestDir, "test.log");
        bool omit = false;
        for (int ii = 0; ii < 2; ++ii)
        {
            using IDevice device = TestUtils.CreateTestDevice(deviceType, filename, omitSegmentIdFromFilename: omit);
            var storageBase = (StorageDeviceBase)device;
            string segmentFilename = storageBase.GetSegmentFilename(filename, 0);
            if (omit)
                Assert.AreEqual(filename, segmentFilename);
            else
                Assert.AreEqual(filename + ".0", segmentFilename);
            omit = true;
        }
    }

    private void TestDeviceWriteRead(IDevice log)
    {
        store = new TsavoriteKV<KeyStruct, ValueStruct>
                   (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10 });

        ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());

        InputStruct input = default;

        for (int i = 0; i < 700; i++)
        {
            var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
            var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
            session.Upsert(ref key1, ref value, Empty.Default, 0);
        }
        session.CompletePending(true);

        // Update first 100 using RMW from storage
        for (int i = 0; i < 100; i++)
        {
            var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
            input = new InputStruct { ifield1 = 1, ifield2 = 1 };
            Status status = session.RMW(ref key1, ref input, Empty.Default, 0);
            if (status.IsPending)
                session.CompletePending(true);
        }


        for (int i = 0; i < 700; i++)
        {
            OutputStruct output = default;
            var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
            var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

            if (session.Read(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
            {
                session.CompletePending(true);
            }
            else
            {
                if (i < 100)
                {
                    Assert.AreEqual(value.vfield1 + 1, output.value.vfield1);
                    Assert.AreEqual(value.vfield2 + 1, output.value.vfield2);
                }
                else
                {
                    Assert.AreEqual(value.vfield1, output.value.vfield1);
                    Assert.AreEqual(value.vfield2, output.value.vfield2);
                }
            }
        }

        session.Dispose();
        store.Dispose();
        store = null;
        log.Dispose();
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
    }
}