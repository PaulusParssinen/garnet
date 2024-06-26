﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;

namespace Tsavorite;

/// <summary>
/// Fixed size pool of overflow objects
/// </summary>
internal sealed class OverflowPool<T> : IDisposable
{
    private readonly int size;
    private readonly ConcurrentQueue<T> itemQueue;
    private readonly Action<T> disposer;

    /// <summary>
    /// Number of pages in pool
    /// </summary>
    public int Count => itemQueue.Count;

    private bool disposed = false;

    /// <summary>
    /// Constructor
    /// </summary>
    public OverflowPool(int size, Action<T> disposer = null)
    {
        this.size = size;
        itemQueue = new ConcurrentQueue<T>();
        this.disposer = disposer ?? (e => { });
    }

    /// <summary>
    /// Try get overflow item, if it exists
    /// </summary>
    public bool TryGet(out T item)
    {
        return itemQueue.TryDequeue(out item);
    }

    /// <summary>
    /// Try to add overflow item to pool
    /// </summary>
    public bool TryAdd(T item)
    {
        if (itemQueue.Count < size && !disposed)
        {
            itemQueue.Enqueue(item);
            return true;
        }
        else
        {
            disposer(item);
            return false;
        }
    }

    /// <summary>
    /// Dispose
    /// </summary>
    public void Dispose()
    {
        disposed = true;
        while (itemQueue.TryDequeue(out T item))
            disposer(item);
    }
}