// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Garnet.server;
using System.Runtime.InteropServices;

namespace BDN.benchmark.Bitmap
{
    public unsafe class BitOperations
    {
        private const int Alignment = 32;

        [ParamsSource(nameof(GetKeySizes))]
        public int[] BitmapSizes { get; set; }

        [Params(BitmapOperation.XOR)]
        public BitmapOperation Op { get; set; }

        public IEnumerable<int[]> GetKeySizes()
        {
            yield return [1 << 21];
            yield return [1 << 21, 1 << 21];
            //yield return [1 << 21, 1 << 21, 1 << 21];
            //yield return [1 << 21, 1 << 21, 1 << 21, 1 << 21];
            //yield return [256, 6 * 512 + 7, 512];
            //yield return [1 << 28, 1 << 28];
            //yield return [1 << 28, 1 << 28, 1 << 28];
        }

        private int minBitmapSize;
        private byte** srcPtrs;
        private byte** srcEndPtrs;

        private int dstLength;
        private byte* dstPtr;

        [GlobalSetup]
        public void GlobalSetup()
        {
            minBitmapSize = BitmapSizes.Min();
            srcPtrs = (byte**)NativeMemory.AllocZeroed((nuint)BitmapSizes.Length, (nuint)sizeof(byte*));
            srcEndPtrs = (byte**)NativeMemory.AllocZeroed((nuint)BitmapSizes.Length, (nuint)sizeof(byte*));

            for (var i = 0; i < BitmapSizes.Length; i++)
            {
                srcPtrs[i] = (byte*)NativeMemory.AlignedAlloc((nuint)BitmapSizes[i], Alignment);
                srcEndPtrs[i] = srcPtrs[i] + BitmapSizes[i];

                new Random(i).NextBytes(new Span<byte>(srcPtrs[i], BitmapSizes[i]));
            }

            dstLength = BitmapSizes.Max();
            dstPtr = (byte*)NativeMemory.AlignedAlloc((nuint)dstLength, Alignment);
        }

        [Benchmark]
        public void BinaryOperation()
        {
            BitmapManager.BitOpMainUnsafeMultiKey(Op, BitmapSizes.Length, srcPtrs, srcEndPtrs, dstPtr, dstLength, minBitmapSize);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            for (var i = 0; i < BitmapSizes.Length; i++)
            {
                NativeMemory.AlignedFree(srcPtrs[i]);
            }

            NativeMemory.Free(srcPtrs);
            NativeMemory.Free(srcEndPtrs);
            NativeMemory.AlignedFree(dstPtr);
        }
    }
}