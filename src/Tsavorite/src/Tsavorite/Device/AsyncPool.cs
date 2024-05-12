// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;

namespace Tsavorite.Device;

/// <summary>
/// Asynchronous pool of fixed pre-filled capacity
/// Supports sync get (TryGet) for fast path
/// </summary>
internal sealed class AsyncPool<T> : IDisposable where T : IDisposable
{
    private readonly int size;
    private readonly Func<T> creator;
    private readonly SemaphoreSlim handleAvailable;
    private readonly ConcurrentQueue<T> itemQueue;
    private bool disposed = false;
    private int totalAllocated = 0;

    /// <summary>
    /// Constructor
    /// </summary>
    public AsyncPool(int size, Func<T> creator)
    {
        this.size = size;
        this.creator = creator;
        handleAvailable = new SemaphoreSlim(0);
        itemQueue = new ConcurrentQueue<T>();
    }

    /// <summary>
    /// Get item synchronously
    /// </summary>
    public T Get(CancellationToken token = default)
    {
        for (; ; )
        {
            if (disposed)
                throw new TsavoriteException("Getting handle in disposed device");

            if (GetOrAdd(itemQueue, out T item))
                return item;

            handleAvailable.Wait(token);
        }
    }

    /// <summary>
    /// Get item asynchronously
    /// </summary>
    public async ValueTask<T> GetAsync(CancellationToken token = default)
    {
        for (; ; )
        {
            if (disposed)
                throw new TsavoriteException("Getting handle in disposed device");

            if (GetOrAdd(itemQueue, out T item))
                return item;

            await handleAvailable.WaitAsync(token).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Try get item (fast path)
    /// </summary>
    public bool TryGet(out T item)
    {
        if (disposed)
        {
            item = default;
            return false;
        }
        return GetOrAdd(itemQueue, out item);
    }

    /// <summary>
    /// Return item to pool
    /// </summary>
    public void Return(T item)
    {
        itemQueue.Enqueue(item);
        if (handleAvailable.CurrentCount < itemQueue.Count)
            handleAvailable.Release();
    }

    /// <summary>
    /// Dispose
    /// </summary>
    public void Dispose()
    {
        disposed = true;

        while (totalAllocated > 0)
        {
            while (itemQueue.TryDequeue(out T item))
            {
                item.Dispose();
                Interlocked.Decrement(ref totalAllocated);
            }
            if (totalAllocated > 0)
                handleAvailable.Wait();
        }
    }

    /// <summary>
    /// Get item from queue, adding up to pool-size items if necessary
    /// </summary>
    private bool GetOrAdd(ConcurrentQueue<T> itemQueue, out T item)
    {
        if (itemQueue.TryDequeue(out item)) return true;

        int _totalAllocated = totalAllocated;
        while (_totalAllocated < size)
        {
            if (Interlocked.CompareExchange(ref totalAllocated, _totalAllocated + 1, _totalAllocated) == _totalAllocated)
            {
                item = creator();
                return true;
            }
            if (itemQueue.TryDequeue(out item)) return true;
            _totalAllocated = totalAllocated;
        }
        return false;
    }
}