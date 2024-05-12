// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;

namespace Tsavorite;

/// <summary>
/// Async queue
/// </summary>
public sealed class AsyncQueue<T>
{
    private readonly SemaphoreSlim semaphore;
    private readonly ConcurrentQueue<T> queue;

    /// <summary>
    /// Queue count
    /// </summary>
    public int Count => queue.Count;

    /// <summary>
    /// Constructor
    /// </summary>
    public AsyncQueue()
    {
        semaphore = new SemaphoreSlim(0);
        queue = new ConcurrentQueue<T>();
    }

    /// <summary>
    /// Enqueue item
    /// </summary>
    public void Enqueue(T item)
    {
        queue.Enqueue(item);
        semaphore.Release();
    }

    /// <summary>
    /// Async dequeue
    /// </summary>
    public async Task<T> DequeueAsync(CancellationToken cancellationToken = default)
    {
        for (; ; )
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            if (queue.TryDequeue(out T item))
            {
                return item;
            }
        }
    }

    /// <summary>
    /// Wait for queue to have at least one entry
    /// </summary>
    public void WaitForEntry() => semaphore.Wait();

    /// <summary>
    /// Wait for queue to have at least one entry
    /// </summary>
    public Task WaitForEntryAsync(CancellationToken token = default) => semaphore.WaitAsync(token);

    /// <summary>
    /// Try dequeue (if item exists)
    /// </summary>
    public bool TryDequeue(out T item)
    {
        return queue.TryDequeue(out item);
    }
}