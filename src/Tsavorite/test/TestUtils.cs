// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Tsavorite.Device;

namespace Tsavorite.Tests;

public static class TestUtils
{
    // Various categories used to group tests
    internal const string SmokeTestCategory = "Smoke";
    internal const string StressTestCategory = "Stress";
    internal const string TsavoriteKVTestCategory = "TsavoriteKV";
    internal const string ReadTestCategory = "Read";
    internal const string LockableUnsafeContextTestCategory = "LockableUnsafeContext";
    internal const string ReadCacheTestCategory = "ReadCache";
    internal const string LockTestCategory = "Locking";
    internal const string LockTableTestCategory = "LockTable";
    internal const string CheckpointRestoreCategory = "CheckpointRestore";
    internal const string MallocFixedPageSizeCategory = "MallocFixedPageSize";
    internal const string RMWTestCategory = "RMW";
    internal const string ModifiedBitTestCategory = "ModifiedBitTest";
    internal const string RevivificationCategory = "Revivification";

    /// <summary>
    /// Delete a directory recursively
    /// </summary>
    /// <param name="path">The folder to delete</param>
    /// <param name="wait">If true, loop on exceptions that are retryable, and verify the directory no longer exists. Generally true on SetUp, false on TearDown</param>
    internal static void DeleteDirectory(string path, bool wait = false)
    {
        while (true)
        {
            try
            {
                if (!Directory.Exists(path))
                    return;
                foreach (string directory in Directory.GetDirectories(path))
                    DeleteDirectory(directory, wait);
                break;
            }
            catch
            {
            }
        }

        for (; ; Thread.Yield())
        {
            // Exceptions may happen due to a handle briefly remaining held after Dispose().
            try
            {
                Directory.Delete(path, true);
            }
            catch (Exception ex) when (ex is IOException ||
                                       ex is UnauthorizedAccessException)
            {
            }
            if (!wait || !Directory.Exists(path))
                break;
        }
    }

    /// <summary>
    /// Create a clean new directory, removing a previous one if needed.
    /// </summary>
    internal static void RecreateDirectory(string path)
    {
        if (Directory.Exists(path))
            DeleteDirectory(path);

        // Don't catch; if this fails, so should the test
        Directory.CreateDirectory(path);
    }

    // Used to test the various devices by using the same test with VALUES parameter
    // Cannot use LocalStorageDevice from non-Windows OS platform
    public enum DeviceType
    {
        LSD,
        MLSD,
        LocalMemory
    }

    internal const int DefaultLocalMemoryDeviceLatencyMs = 20;   // latencyMs only applies to DeviceType = LocalMemory

    internal static IDevice CreateTestDevice(DeviceType testDeviceType, string filename, int latencyMs = DefaultLocalMemoryDeviceLatencyMs, bool deleteOnClose = false, bool omitSegmentIdFromFilename = false)
    {
        IDevice device = null;
        bool preallocateFile = false;
        long capacity = Devices.CAPACITY_UNSPECIFIED;
        bool recoverDevice = false;

        switch (testDeviceType)
        {
            case DeviceType.LSD when !OperatingSystem.IsWindows():
                Assert.Ignore($"Skipping {nameof(DeviceType.LSD)} on non-Windows platforms");
                break;
            case DeviceType.LSD when OperatingSystem.IsWindows():
                bool useIoCompletionPort = false;
                bool disableFileBuffering = true;
                device = new LocalStorageDevice(filename, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort);
                break;
            case DeviceType.MLSD:
                device = new ManagedLocalStorageDevice(filename, preallocateFile, deleteOnClose, true, capacity, recoverDevice);
                break;
            // Emulated higher latency storage device - takes a disk latency arg (latencyMs) and emulates an IDevice using main memory, serving data at specified latency
            case DeviceType.LocalMemory:
                device = new LocalMemoryDevice(1L << 28, 1L << 25, 2, sector_size: 512, latencyMs: latencyMs);  // 64 MB (1L << 26) is enough for our test cases
                break;
        }

        if (omitSegmentIdFromFilename)
            device.Initialize(segmentSize: -1L, omitSegmentIdFromFilename: omitSegmentIdFromFilename);
        return device;
    }

    private static string ConvertedClassName()
    {
        // Make this all under one root folder named {prefix}, which is the base namespace name. All UT namespaces using this must start with this prefix.
        const string prefix = "Tsavorite.Tests";
        const string prefix2 = "NUnit.Framework.Internal.TestExecutionContext";

        if (TestContext.CurrentContext.Test.ClassName.StartsWith($"{prefix}."))
        {
            return TestContext.CurrentContext.Test.ClassName.Substring(prefix.Length + 1);
        }
        else if (TestContext.CurrentContext.Test.ClassName.StartsWith($"{prefix2}+"))
        {
            return TestContext.CurrentContext.Test.ClassName.Substring(prefix2.Length + 1);
        }
        else
        {
            Assert.Fail($"Expected {prefix} prefix was not found");
            return "";
        }
    }

    // Tsavorite paths are too long; as a workaround (possibly temporary) remove the class name (many long test method names repeat much of the class name).
    //internal static string MethodTestDir => Path.Combine(TestContext.CurrentContext.TestDirectory, $"{ConvertedClassName()}_{TestContext.CurrentContext.Test.MethodName}");
    internal static string MethodTestDir => Path.Combine(TestContext.CurrentContext.TestDirectory, $"Tsavorite.Tests/{TestContext.CurrentContext.Test.MethodName}");

    public enum AllocatorType
    {
        FixedBlittable,
        SpanByte,
        Generic
    }

    internal enum SyncMode { Sync, Async }

    public enum ReadCopyDestination { Tail, ReadCache }

    public enum FlushMode { NoFlush, ReadOnly, OnDisk }

    public enum KeyEquality { Equal, NotEqual }

    public enum ReadCacheMode { UseReadCache, NoReadCache }

    public enum KeyContentionMode { Contention, NoContention }

    public enum BatchMode { Batch, NoBatch }

    public enum UpdateOp { Upsert, RMW, Delete }

    public enum HashModulo { NoMod = 0, Hundred = 100, Thousand = 1000 }

    public enum ScanIteratorType { Pull, Push }

    public enum ScanMode { Scan, Iterate }

    public enum WaitMode { Wait, NoWait }

    internal static (Status status, TOutput output) GetSinglePendingResult<TKey, TValue, TInput, TOutput, TContext>(CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs)
        => GetSinglePendingResult(completedOutputs, out _);

    internal static (Status status, TOutput output) GetSinglePendingResult<TKey, TValue, TInput, TOutput, TContext>(CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, out RecordMetadata recordMetadata)
    {
        Assert.IsTrue(completedOutputs.Next());
        (Status Status, TOutput Output) result = (completedOutputs.Current.Status, completedOutputs.Current.Output);
        recordMetadata = completedOutputs.Current.RecordMetadata;
        Assert.IsFalse(completedOutputs.Next());
        completedOutputs.Dispose();
        return result;
    }

    internal static async ValueTask<(Status status, Output output)> CompleteAsync<Key, Value, Input, Output, Context>(ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> resultTask)
    {
        TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context> readCompleter = await resultTask;
        return readCompleter.Complete();
    }

    internal static async ValueTask<Status> CompleteAsync<Key, Value, Context>(ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Key, Value, Context>> resultTask)
    {
        TsavoriteKV<Key, Value>.UpsertAsyncResult<Key, Value, Context> result = await resultTask;
        while (result.Status.IsPending)
            result = await result.CompleteAsync().ConfigureAwait(false);
        return result.Status;
    }

    internal static async ValueTask<Status> CompleteAsync<Key, Value, Context>(ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Value, Value, Context>> resultTask)
    {
        TsavoriteKV<Key, Value>.RmwAsyncResult<Value, Value, Context> result = await resultTask;
        while (result.Status.IsPending)
            result = await result.CompleteAsync().ConfigureAwait(false);
        return result.Status;
    }

    internal static async ValueTask<Status> CompleteAsync<Key, Value, Input, Output, Context>(ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> resultTask)
    {
        TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context> deleteCompleter = await resultTask;
        return deleteCompleter.Complete();
    }

    internal static async ValueTask DoTwoThreadRandomKeyTest(int count, bool doRandom, Action<int> first, Action<int> second, Action<int> verification)
    {
        Task[] tasks = new Task[2];

        var rng = new Random(101);
        for (int iter = 0; iter < count; ++iter)
        {
            int arg = doRandom ? rng.Next(count) : iter;
            tasks[0] = Task.Factory.StartNew(() => first(arg));
            tasks[1] = Task.Factory.StartNew(() => second(arg));

            await Task.WhenAll(tasks);

            verification(arg);
        }
    }

    internal static unsafe bool FindHashBucketEntryForKey<Key, Value>(this TsavoriteKV<Key, Value> store, ref Key key, out HashBucketEntry entry)
    {
        HashEntryInfo hei = new(store.Comparer.GetHashCode64(ref key));
        bool success = store.FindTag(ref hei);
        entry = hei.entry;
        return success;
    }
}