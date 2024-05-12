// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace Garnet.Common;

internal class LightConcurrentStack<T> : IDisposable
    where T : class, IDisposable
{
    private readonly T[] stack;
    private int tail;
    private SpinLock latch;
    private bool disposed;

    public LightConcurrentStack(int maxCapacity = 128)
    {
        stack = new T[maxCapacity];
        tail = 0;
        latch = new SpinLock();
        disposed = false;
    }

    public void Dispose()
    {
        bool lockTaken = false;
        latch.Enter(ref lockTaken);
        Debug.Assert(lockTaken);
        disposed = true;
        while (tail > 0)
        {
            T elem = stack[--tail];
            elem.Dispose();
        }
        latch.Exit();
    }

    public bool TryPush(T elem)
    {
        bool lockTaken = false;
        latch.Enter(ref lockTaken);
        Debug.Assert(lockTaken);
        if (disposed || tail == stack.Length)
        {
            latch.Exit();
            return false;
        }
        stack[tail++] = elem;
        latch.Exit();
        return true;
    }

    public bool TryPop(out T elem, out bool disposed)
    {
        elem = null;
        bool lockTaken = false;
        latch.Enter(ref lockTaken);
        Debug.Assert(lockTaken);
        disposed = this.disposed;
        if (tail == 0)
        {
            latch.Exit();
            return false;
        }

        elem = stack[--tail];
        latch.Exit();
        return true;
    }
}