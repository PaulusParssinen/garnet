﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite
{
    /// <summary>
    /// A frame is an in-memory circular buffer of log pages
    /// </summary>
    internal sealed class GenericFrame<Key, Value> : IDisposable
    {
        private readonly Record<Key, Value>[][] frame;
        public readonly int frameSize, pageSize;
        private static int RecordSize => Unsafe.SizeOf<Record<Key, Value>>();

        public GenericFrame(int frameSize, int pageSize)
        {
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            frame = new Record<Key, Value>[frameSize][];
        }

        public void Allocate(int index)
        {
            frame[index] = new Record<Key, Value>[(pageSize + RecordSize - 1) / RecordSize];
        }

        public void Clear(int pageIndex)
        {
            Array.Clear(frame[pageIndex], 0, frame[pageIndex].Length);
        }

        public ref Key GetKey(long frameNumber, long offset)
        {
            return ref frame[frameNumber][offset].key;
        }

        public ref Value GetValue(long frameNumber, long offset)
        {
            return ref frame[frameNumber][offset].value;
        }

        public ref RecordInfo GetInfo(long frameNumber, long offset)
        {
            return ref frame[frameNumber][offset].info;
        }

        public ref Record<Key, Value>[] GetPage(long frameNumber)
        {
            return ref frame[frameNumber];
        }

        public void Dispose()
        {
            Array.Clear(frame, 0, frame.Length);
        }
    }
}