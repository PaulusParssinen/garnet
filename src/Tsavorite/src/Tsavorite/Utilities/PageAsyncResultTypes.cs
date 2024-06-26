﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define CALLOC

using Tsavorite.Device;

namespace Tsavorite;

/// <summary>
/// Result of async page read
/// </summary>
public sealed class PageAsyncReadResult<TContext>
{
    internal long page;
    internal long offset;
    internal TContext context;
    internal CountdownEvent handle;
    internal SectorAlignedMemory freeBuffer1;
    internal SectorAlignedMemory freeBuffer2;
    internal DeviceIOCompletionCallback callback;
    internal IDevice objlogDevice;
    internal object frame;
    internal CancellationTokenSource cts;

    /* Used for iteration */
    internal long resumePtr;
    internal long untilPtr;
    internal long maxPtr;

    /// <summary>
    /// Free
    /// </summary>
    public void Free()
    {
        if (freeBuffer1 != null)
        {
            freeBuffer1.Return();
            freeBuffer1 = null;
        }

        if (freeBuffer2 != null)
        {
            freeBuffer2.Return();
            freeBuffer2 = null;
        }
    }
}

/// <summary>
/// Shared flush completion tracker, when bulk-flushing many pages
/// </summary>
internal sealed class FlushCompletionTracker
{
    /// <summary>
    /// Semaphore to set on flush completion
    /// </summary>
    private readonly SemaphoreSlim completedSemaphore;

    /// <summary>
    /// Semaphore to wait on for flush completion
    /// </summary>
    private readonly SemaphoreSlim flushSemaphore;

    /// <summary>
    /// Number of pages being flushed
    /// </summary>
    private int count;

    /// <summary>
    /// Create a flush completion tracker
    /// </summary>
    /// <param name="completedSemaphore">Semaphpore to release when all flushes completed</param>
    /// <param name="flushSemaphore">Semaphpore to release when each flush completes</param>
    /// <param name="count">Number of pages to flush</param>
    public FlushCompletionTracker(SemaphoreSlim completedSemaphore, SemaphoreSlim flushSemaphore, int count)
    {
        this.completedSemaphore = completedSemaphore;
        this.flushSemaphore = flushSemaphore;
        this.count = count;
    }

    /// <summary>
    /// Complete flush of one page
    /// </summary>
    public void CompleteFlush()
    {
        flushSemaphore?.Release();
        if (Interlocked.Decrement(ref count) == 0)
            completedSemaphore.Release();
    }

    public void WaitOneFlush()
        => flushSemaphore?.Wait();
}

/// <summary>
/// Page async flush result
/// </summary>
public sealed class PageAsyncFlushResult<TContext>
{
    /// <summary>
    /// Page
    /// </summary>
    public long page;
    /// <summary>
    /// Context
    /// </summary>
    public TContext context;
    /// <summary>
    /// Count
    /// </summary>
    public int count;

    internal bool partial;
    internal long fromAddress;
    internal long untilAddress;
    internal SectorAlignedMemory freeBuffer1;
    internal SectorAlignedMemory freeBuffer2;
    internal AutoResetEvent done;
    internal FlushCompletionTracker flushCompletionTracker;

    /// <summary>
    /// Free
    /// </summary>
    public void Free()
    {
        if (freeBuffer1 != null)
        {
            freeBuffer1.Return();
            freeBuffer1 = null;
        }
        if (freeBuffer2 != null)
        {
            freeBuffer2.Return();
            freeBuffer2 = null;
        }

        flushCompletionTracker?.CompleteFlush();
    }
}