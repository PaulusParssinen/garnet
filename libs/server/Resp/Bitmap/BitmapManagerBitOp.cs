// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Garnet.common.Numerics;

namespace Garnet.server
{
    public unsafe partial class BitmapManager
    {
        /// <summary>
        /// BitOp main driver.
        /// </summary>
        /// <param name="op">Type of the operation being executed</param>
        /// <param name="srcCount">Number of source buffers</param>
        /// <param name="srcPtrs">Array of pointers to source buffers. The array length must be greater than or equal to <paramref name="srcCount"/></param>
        /// <param name="srcEndPtrs">Array of the buffer lengths specified in <paramref name="srcPtrs"/>. The array length must be greater than or equal to <paramref name="srcCount"/></param>
        /// <param name="dstPtr">Destination buffer to write the result</param>
        /// <param name="dstLength">Destination buffer length</param>
        /// <param name="shortestSrcLength">The length of shorted source buffer</param>
        /// <returns></returns>
        public static void BitOpMainUnsafeMultiKey(BitmapOperation op, int srcCount, byte** srcPtrs, byte** srcEndPtrs, byte* dstPtr, int dstLength, int shortestSrcLength)
        {
            Debug.Assert(srcCount > 0);
            Debug.Assert(op is BitmapOperation.NOT or BitmapOperation.AND or BitmapOperation.OR or BitmapOperation.XOR);

            if (srcCount == 1)
            {
                var srcKey = new ReadOnlySpan<byte>(srcPtrs[0], checked((int)(srcEndPtrs[0] - srcPtrs[0])));
                var dstKey = new Span<byte>(dstPtr, dstLength);

                if (op == BitmapOperation.NOT)
                {
                    TensorPrimitives.OnesComplement(srcKey, dstKey);
                }
                else
                {
                    srcKey.CopyTo(dstKey);
                }
            }
            else if (srcCount == 2)
            {
                var firstKeyLength = checked((int)(srcEndPtrs[0] - srcPtrs[0]));
                var secondKeyLength = checked((int)(srcEndPtrs[1] - srcPtrs[1]));
                var srcFirstKey = new ReadOnlySpan<byte>(srcPtrs[0], firstKeyLength);
                var srcSecondKey = new ReadOnlySpan<byte>(srcPtrs[1], secondKeyLength);
                var dstKey = new Span<byte>(dstPtr, dstLength);

                if (op == BitmapOperation.AND) TensorPrimitives.BitwiseAnd(srcFirstKey, srcSecondKey, dstKey);
                else if (op == BitmapOperation.OR) TensorPrimitives.BitwiseOr(srcFirstKey, srcSecondKey, dstKey);
                else if (op == BitmapOperation.XOR) TensorPrimitives.Xor(srcFirstKey, srcSecondKey, dstKey);
            }
            else
            {
                if (op == BitmapOperation.AND) InvokeMultiKeyBitwise<BitwiseAndOperator>(srcCount, srcPtrs, srcEndPtrs, dstPtr, dstLength, shortestSrcLength);
                else if (op == BitmapOperation.OR) InvokeMultiKeyBitwise<BitwiseOrOperator>(srcCount, srcPtrs, srcEndPtrs, dstPtr, dstLength, shortestSrcLength);
                else if (op == BitmapOperation.XOR) InvokeMultiKeyBitwise<BitwiseXorOperator>(srcCount, srcPtrs, srcEndPtrs, dstPtr, dstLength, shortestSrcLength);
            }
        }

        /// <summary>
        /// Invokes bitwise binary operation for multiple keys.
        /// </summary>
        /// <typeparam name="TBinaryOperator">The binary operator type to compute bitwise</typeparam>
        /// <param name="srcCount">Number of source buffers</param>
        /// <param name="srcPtrs">Array of pointers to source buffers. The array length must be greater than or equal to <paramref name="srcCount"/></param>
        /// <param name="srcEndPtrs">Array of the buffer lengths specified in <paramref name="srcPtrs"/>. The array length must be greater than or equal to <paramref name="srcCount"/></param>
        /// <param name="dstPtr">Destination buffer to write the result</param>
        /// <param name="dstLength">Destination buffer length</param>
        /// <param name="shortestSrcLength">The length of shorted source buffer</param>
        [SkipLocalsInit]
        private static void InvokeMultiKeyBitwise<TBinaryOperator>(int srcCount, byte** srcPtrs, byte** srcEndPtrs, byte* dstPtr, int dstLength, int shortestSrcLength)
            where TBinaryOperator : struct, IBinaryOperator
        {
            Debug.Assert(srcCount > 2);
            Debug.Assert(dstLength >= shortestSrcLength);

            var dstCurrentPtr = dstPtr;

            long remainingLength = shortestSrcLength;
            long batchRemainder = shortestSrcLength;
            byte* dstBatchEndPtr;

            // Keep the cursor of the first source buffer in local to keep processing tidy.
            var firstSrcPtr = srcPtrs[0];

            // Copy remaining source buffer pointers so we don't increment caller's.
            var srcCurrentPtrs = stackalloc byte*[srcCount];
            for (var i = 0; i < srcCount; i++)
            {
                srcCurrentPtrs[i] = srcPtrs[i];
            }

            if (Vector256.IsHardwareAccelerated && Vector256<byte>.IsSupported)
            {
                // Vectorized: 32 bytes x 8
                batchRemainder = remainingLength & ((Vector256<byte>.Count * 8) - 1);
                dstBatchEndPtr = dstCurrentPtr + (remainingLength - batchRemainder);
                remainingLength = batchRemainder;

                Vectorized256(ref firstSrcPtr, srcCount, srcCurrentPtrs, ref dstCurrentPtr, dstBatchEndPtr);
            }
            else if (Vector128.IsHardwareAccelerated && Vector128<byte>.IsSupported)
            {
                // Vectorized: 16 bytes x 8
                batchRemainder = remainingLength & ((Vector128<byte>.Count * 8) - 1);
                dstBatchEndPtr = dstCurrentPtr + (remainingLength - batchRemainder);
                remainingLength = batchRemainder;

                Vectorized128(ref firstSrcPtr, srcCount, srcCurrentPtrs, ref dstCurrentPtr, dstBatchEndPtr);
            }

            // Scalar: 8 bytes x 4
            batchRemainder = remainingLength & ((sizeof(ulong) * 4) - 1);
            dstBatchEndPtr = dstCurrentPtr + (remainingLength - batchRemainder);
            remainingLength = batchRemainder;

            while (dstCurrentPtr < dstBatchEndPtr)
            {
                var d00 = *(ulong*)(firstSrcPtr + (sizeof(ulong) * 0));
                var d01 = *(ulong*)(firstSrcPtr + (sizeof(ulong) * 1));
                var d02 = *(ulong*)(firstSrcPtr + (sizeof(ulong) * 2));
                var d03 = *(ulong*)(firstSrcPtr + (sizeof(ulong) * 3));

                firstSrcPtr += sizeof(ulong) * 4;

                for (var i = 1; i < srcCount; i++)
                {
                    ref var keyStartPtr = ref srcCurrentPtrs[i];

                    d00 = TBinaryOperator.Invoke(d00, *(ulong*)(keyStartPtr + (sizeof(ulong) * 0)));
                    d01 = TBinaryOperator.Invoke(d01, *(ulong*)(keyStartPtr + (sizeof(ulong) * 1)));
                    d02 = TBinaryOperator.Invoke(d02, *(ulong*)(keyStartPtr + (sizeof(ulong) * 2)));
                    d03 = TBinaryOperator.Invoke(d03, *(ulong*)(keyStartPtr + (sizeof(ulong) * 3)));

                    keyStartPtr += sizeof(ulong) * 4;
                }

                *(ulong*)(dstCurrentPtr + (sizeof(ulong) * 0)) = d00;
                *(ulong*)(dstCurrentPtr + (sizeof(ulong) * 1)) = d01;
                *(ulong*)(dstCurrentPtr + (sizeof(ulong) * 2)) = d02;
                *(ulong*)(dstCurrentPtr + (sizeof(ulong) * 3)) = d03;

                dstCurrentPtr += sizeof(ulong) * 4;
            }

            // Handle the remaining tails
            var dstEndPtr = dstPtr + dstLength;
            while (dstCurrentPtr < dstEndPtr)
            {
                byte d00 = 0;

                if (firstSrcPtr < srcEndPtrs[0])
                {
                    d00 = *firstSrcPtr;
                    firstSrcPtr++;
                }
                else if (typeof(TBinaryOperator) == typeof(BitwiseAndOperator))
                {
                    goto write;
                }

                for (var i = 1; i < srcCount; i++)
                {
                    if (srcCurrentPtrs[i] < srcEndPtrs[i])
                    {
                        d00 = TBinaryOperator.Invoke(d00, *srcCurrentPtrs[i]);
                        srcCurrentPtrs[i]++;
                    }
                    else if (typeof(TBinaryOperator) == typeof(BitwiseAndOperator))
                    {
                        d00 = 0;
                        goto write;
                    }
                }

            write:
                *dstCurrentPtr++ = d00;
            }

            static void Vectorized256(ref byte* firstSrcPtr, int srcCount, byte** srcCurrentPtrs, ref byte* dstCurrentPtr, byte* dstBatchEndPtr)
            {
                while (dstCurrentPtr < dstBatchEndPtr)
                {
                    var d00 = Vector256.Load(firstSrcPtr + (Vector256<byte>.Count * 0));
                    var d01 = Vector256.Load(firstSrcPtr + (Vector256<byte>.Count * 1));
                    var d02 = Vector256.Load(firstSrcPtr + (Vector256<byte>.Count * 2));
                    var d03 = Vector256.Load(firstSrcPtr + (Vector256<byte>.Count * 3));
                    var d04 = Vector256.Load(firstSrcPtr + (Vector256<byte>.Count * 4));
                    var d05 = Vector256.Load(firstSrcPtr + (Vector256<byte>.Count * 5));
                    var d06 = Vector256.Load(firstSrcPtr + (Vector256<byte>.Count * 6));
                    var d07 = Vector256.Load(firstSrcPtr + (Vector256<byte>.Count * 7));

                    firstSrcPtr += Vector256<byte>.Count * 8;

                    for (var i = 1; i < srcCount; i++)
                    {
                        ref var keyStartPtr = ref srcCurrentPtrs[i];

                        var s00 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 0));
                        var s01 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 1));
                        var s02 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 2));
                        var s03 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 3));
                        var s04 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 4));
                        var s05 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 5));
                        var s06 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 6));
                        var s07 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 7));

                        d00 = TBinaryOperator.Invoke(d00, s00);
                        d01 = TBinaryOperator.Invoke(d01, s01);
                        d02 = TBinaryOperator.Invoke(d02, s02);
                        d03 = TBinaryOperator.Invoke(d03, s03);
                        d04 = TBinaryOperator.Invoke(d04, s04);
                        d05 = TBinaryOperator.Invoke(d05, s05);
                        d06 = TBinaryOperator.Invoke(d06, s06);
                        d07 = TBinaryOperator.Invoke(d07, s07);

                        keyStartPtr += Vector256<byte>.Count * 8;
                    }

                    Vector256.Store(d00, dstCurrentPtr + (Vector256<byte>.Count * 0));
                    Vector256.Store(d01, dstCurrentPtr + (Vector256<byte>.Count * 1));
                    Vector256.Store(d02, dstCurrentPtr + (Vector256<byte>.Count * 2));
                    Vector256.Store(d03, dstCurrentPtr + (Vector256<byte>.Count * 3));
                    Vector256.Store(d04, dstCurrentPtr + (Vector256<byte>.Count * 4));
                    Vector256.Store(d05, dstCurrentPtr + (Vector256<byte>.Count * 5));
                    Vector256.Store(d06, dstCurrentPtr + (Vector256<byte>.Count * 6));
                    Vector256.Store(d07, dstCurrentPtr + (Vector256<byte>.Count * 7));

                    dstCurrentPtr += Vector256<byte>.Count * 8;
                }
            }

            static void Vectorized128(ref byte* firstSrcPtr, int srcCount, byte** srcCurrentPtrs, ref byte* dstCurrentPtr, byte* dstBatchEndPtr)
            {
                while (dstCurrentPtr < dstBatchEndPtr)
                {
                    var d00 = Vector128.Load(firstSrcPtr + (Vector128<byte>.Count * 0));
                    var d01 = Vector128.Load(firstSrcPtr + (Vector128<byte>.Count * 1));
                    var d02 = Vector128.Load(firstSrcPtr + (Vector128<byte>.Count * 2));
                    var d03 = Vector128.Load(firstSrcPtr + (Vector128<byte>.Count * 3));
                    var d04 = Vector128.Load(firstSrcPtr + (Vector128<byte>.Count * 4));
                    var d05 = Vector128.Load(firstSrcPtr + (Vector128<byte>.Count * 5));
                    var d06 = Vector128.Load(firstSrcPtr + (Vector128<byte>.Count * 6));
                    var d07 = Vector128.Load(firstSrcPtr + (Vector128<byte>.Count * 7));

                    firstSrcPtr += Vector128<byte>.Count * 8;

                    for (var i = 1; i < srcCount; i++)
                    {
                        ref var keyStartPtr = ref srcCurrentPtrs[i];

                        var s00 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 0));
                        var s01 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 1));
                        var s02 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 2));
                        var s03 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 3));
                        var s04 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 4));
                        var s05 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 5));
                        var s06 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 6));
                        var s07 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 7));

                        d00 = TBinaryOperator.Invoke(d00, s00);
                        d01 = TBinaryOperator.Invoke(d01, s01);
                        d02 = TBinaryOperator.Invoke(d02, s02);
                        d03 = TBinaryOperator.Invoke(d03, s03);
                        d04 = TBinaryOperator.Invoke(d04, s04);
                        d05 = TBinaryOperator.Invoke(d05, s05);
                        d06 = TBinaryOperator.Invoke(d06, s06);
                        d07 = TBinaryOperator.Invoke(d07, s07);

                        keyStartPtr += Vector128<byte>.Count * 8;
                    }

                    Vector128.Store(d00, dstCurrentPtr + (Vector128<byte>.Count * 0));
                    Vector128.Store(d01, dstCurrentPtr + (Vector128<byte>.Count * 1));
                    Vector128.Store(d02, dstCurrentPtr + (Vector128<byte>.Count * 2));
                    Vector128.Store(d03, dstCurrentPtr + (Vector128<byte>.Count * 3));
                    Vector128.Store(d04, dstCurrentPtr + (Vector128<byte>.Count * 4));
                    Vector128.Store(d05, dstCurrentPtr + (Vector128<byte>.Count * 5));
                    Vector128.Store(d06, dstCurrentPtr + (Vector128<byte>.Count * 6));
                    Vector128.Store(d07, dstCurrentPtr + (Vector128<byte>.Count * 7));

                    dstCurrentPtr += Vector128<byte>.Count * 8;
                }
            }
        }
    }
}