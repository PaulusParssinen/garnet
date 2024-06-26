﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using static Tsavorite.Tests.TestUtils;

namespace Tsavorite.Tests.async;

[TestFixture]
internal class SessionTests
{
    private TsavoriteKV<KeyStruct, ValueStruct> store;
    private IDevice log;

    [SetUp]
    public void Setup()
    {
        DeleteDirectory(MethodTestDir, wait: true);
        log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog1.log"), deleteOnClose: true);
        store = new TsavoriteKV<KeyStruct, ValueStruct>
            (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
    }

    [TearDown]
    public void TearDown()
    {
        store?.Dispose();
        store = null;
        log?.Dispose();
        log = null;
        DeleteDirectory(MethodTestDir);
    }

    [Test]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    public void SessionTest1()
    {
        using ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
        InputStruct input = default;
        OutputStruct output = default;

        var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
        var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

        session.Upsert(ref key1, ref value, Empty.Default, 0);
        Status status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);

        if (status.IsPending)
        {
            session.CompletePendingWithOutputs(out CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs, wait: true);
            (status, output) = GetSinglePendingResult(outputs);
        }

        Assert.IsTrue(status.Found);
        Assert.AreEqual(value.vfield1, output.value.vfield1);
        Assert.AreEqual(value.vfield2, output.value.vfield2);
    }

    [Test]
    [Category("TsavoriteKV")]
    public void SessionTest2()
    {
        using ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> session1 = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
        using ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> session2 = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
        InputStruct input = default;
        OutputStruct output = default;

        var key1 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
        var value1 = new ValueStruct { vfield1 = 24, vfield2 = 25 };
        var key2 = new KeyStruct { kfield1 = 15, kfield2 = 16 };
        var value2 = new ValueStruct { vfield1 = 25, vfield2 = 26 };

        session1.Upsert(ref key1, ref value1, Empty.Default, 0);
        session2.Upsert(ref key2, ref value2, Empty.Default, 0);

        Status status = session1.Read(ref key1, ref input, ref output, Empty.Default, 0);

        if (status.IsPending)
        {
            session1.CompletePendingWithOutputs(out CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs, wait: true);
            (status, output) = GetSinglePendingResult(outputs);
        }

        Assert.IsTrue(status.Found);
        Assert.AreEqual(value1.vfield1, output.value.vfield1);
        Assert.AreEqual(value1.vfield2, output.value.vfield2);

        status = session2.Read(ref key2, ref input, ref output, Empty.Default, 0);

        if (status.IsPending)
        {
            session2.CompletePendingWithOutputs(out CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs, wait: true);
            (status, output) = GetSinglePendingResult(outputs);
        }

        Assert.IsTrue(status.Found);
        Assert.AreEqual(value2.vfield1, output.value.vfield1);
        Assert.AreEqual(value2.vfield2, output.value.vfield2);
    }

    [Test]
    [Category("TsavoriteKV")]
    public void SessionTest3()
    {
        using ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
        Task.CompletedTask.ContinueWith((t) =>
        {
            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            Status status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);

            if (status.IsPending)
            {
                session.CompletePendingWithOutputs(out CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }

            Assert.IsTrue(status.Found);
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
        }).Wait();
    }

    [Test]
    [Category("TsavoriteKV")]
    public void SessionTest4()
    {
        using ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> session1 = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
        using ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> session2 = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
        Task t1 = Task.CompletedTask.ContinueWith((t) =>
        {
            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
            var value1 = new ValueStruct { vfield1 = 24, vfield2 = 25 };

            session1.Upsert(ref key1, ref value1, Empty.Default, 0);
            Status status = session1.Read(ref key1, ref input, ref output, Empty.Default, 0);

            if (status.IsPending)
            {
                session1.CompletePendingWithOutputs(out CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }

            Assert.IsTrue(status.Found);
            Assert.AreEqual(value1.vfield1, output.value.vfield1);
            Assert.AreEqual(value1.vfield2, output.value.vfield2);
        });

        Task t2 = Task.CompletedTask.ContinueWith((t) =>
        {
            InputStruct input = default;
            OutputStruct output = default;

            var key2 = new KeyStruct { kfield1 = 15, kfield2 = 16 };
            var value2 = new ValueStruct { vfield1 = 25, vfield2 = 26 };

            session2.Upsert(ref key2, ref value2, Empty.Default, 0);

            Status status = session2.Read(ref key2, ref input, ref output, Empty.Default, 0);

            if (status.IsPending)
            {
                session2.CompletePendingWithOutputs(out CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }

            Assert.IsTrue(status.Found);
            Assert.AreEqual(value2.vfield1, output.value.vfield1);
            Assert.AreEqual(value2.vfield2, output.value.vfield2);
        });

        t1.Wait();
        t2.Wait();
    }

    [Test]
    [Category("TsavoriteKV")]
    public void SessionTest5()
    {
        ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());

        InputStruct input = default;
        OutputStruct output = default;

        var key1 = new KeyStruct { kfield1 = 16, kfield2 = 17 };
        var value1 = new ValueStruct { vfield1 = 26, vfield2 = 27 };

        session.Upsert(ref key1, ref value1, Empty.Default, 0);
        Status status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);

        if (status.IsPending)
        {
            session.CompletePendingWithOutputs(out CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs, wait: true);
            (status, output) = GetSinglePendingResult(outputs);
        }

        Assert.IsTrue(status.Found);
        Assert.AreEqual(value1.vfield1, output.value.vfield1);
        Assert.AreEqual(value1.vfield2, output.value.vfield2);

        session.Dispose();

        session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());

        var key2 = new KeyStruct { kfield1 = 17, kfield2 = 18 };
        var value2 = new ValueStruct { vfield1 = 27, vfield2 = 28 };

        session.Upsert(ref key2, ref value2, Empty.Default, 0);

        status = session.Read(ref key2, ref input, ref output, Empty.Default, 0);

        if (status.IsPending)
        {
            session.CompletePendingWithOutputs(out CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs, wait: true);
            (status, output) = GetSinglePendingResult(outputs);
        }
        Assert.IsTrue(status.Found);

        status = session.Read(ref key2, ref input, ref output, Empty.Default, 0);

        if (status.IsPending)
        {
            session.CompletePendingWithOutputs(out CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs, wait: true);
            (status, output) = GetSinglePendingResult(outputs);
        }

        Assert.IsTrue(status.Found);
        Assert.AreEqual(value2.vfield1, output.value.vfield1);
        Assert.AreEqual(value2.vfield2, output.value.vfield2);

        session.Dispose();
    }
}