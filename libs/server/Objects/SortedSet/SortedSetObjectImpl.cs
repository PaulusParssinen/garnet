﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Sorted Set - RESP specific operations
    /// </summary>
    public unsafe partial class SortedSetObject : GarnetObjectBase
    {
        /// <summary>
        /// Small struct to store options for ZRange command
        /// </summary>
        private struct ZRangeOptions
        {
            public bool ByScore { get; set; }
            public bool ByLex { get; set; }
            public bool Reverse { get; set; }
            public (int, int) Limit { get; set; }
            public bool ValidLimit { get; set; }
            public bool WithScores { get; set; }
        };

        bool GetOptions(ref ObjectInput input, ref int currTokenIdx, out SortedSetAddOption options, ref byte* curr, byte* end, ref SpanByteAndMemory output, ref bool isMemory, ref byte* ptr, ref MemoryHandle ptrHandle)
        {
            options = SortedSetAddOption.None;

            while (currTokenIdx < input.parseState.Count)
            {
                if (!input.parseState.TryGetSortedSetAddOption(currTokenIdx, out var currOption))
                    break;

                options |= currOption;
                currTokenIdx++;
            }

            // Validate ZADD options combination
            ReadOnlySpan<byte> optionsError = default;

            // XX & NX are mutually exclusive
            if ((options & SortedSetAddOption.XX) == SortedSetAddOption.XX &&
                (options & SortedSetAddOption.NX) == SortedSetAddOption.NX)
                optionsError = CmdStrings.RESP_ERR_XX_NX_NOT_COMPATIBLE;

            // NX, GT & LT are mutually exclusive
            if (((options & SortedSetAddOption.GT) == SortedSetAddOption.GT &&
                 (options & SortedSetAddOption.LT) == SortedSetAddOption.LT) ||
               (((options & SortedSetAddOption.GT) == SortedSetAddOption.GT ||
                 (options & SortedSetAddOption.LT) == SortedSetAddOption.LT) &&
                (options & SortedSetAddOption.NX) == SortedSetAddOption.NX))
                optionsError = CmdStrings.RESP_ERR_GT_LT_NX_NOT_COMPATIBLE;

            // INCR supports only one score-element pair
            if ((options & SortedSetAddOption.INCR) == SortedSetAddOption.INCR &&
                (input.parseState.Count - currTokenIdx > 2))
                optionsError = CmdStrings.RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR;

            if (!optionsError.IsEmpty)
            {
                while (!RespWriteUtils.TryWriteError(optionsError, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                return false;
            }

            // From here on we expect only score-element pairs
            // Remaining token count should be positive and even
            if (currTokenIdx == input.parseState.Count || (input.parseState.Count - currTokenIdx) % 2 != 0)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                return false;
            }

            return true;
        }

        private void SortedSetAdd(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;
            var addedOrChanged = 0;
            double incrResult = 0;

            try
            {
                var options = SortedSetAddOption.None;
                var currTokenIdx = 0;
                var parsedOptions = false;

                while (currTokenIdx < input.parseState.Count)
                {
                    // Try to parse a Score field
                    if (!input.parseState.TryGetDouble(currTokenIdx, out var score))
                    {
                        // Try to get and validate options before the Score field, if any
                        if (!parsedOptions)
                        {
                            parsedOptions = true;
                            if (!GetOptions(ref input, ref currTokenIdx, out options, ref curr, end, ref output, ref isMemory, ref ptr, ref ptrHandle))
                                return;
                            continue; // retry after parsing options
                        }
                        else
                        {
                            // Invalid Score encountered
                            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            return;
                        }
                    }

                    parsedOptions = true;
                    currTokenIdx++;

                    // Copy the member
                    var member = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan.ToArray();
                    
                    // Avoid multiple hash calculations
                    ref var scoreRef = ref CollectionsMarshal.GetValueRefOrAddDefault(Dictionary, member, out var exists);

                    // Add new member
                    if (!exists)
                    {
                        // Don't add new member if XX flag is set
                        if ((options & SortedSetAddOption.XX) == SortedSetAddOption.XX) continue;

                        scoreRef = score;
                        if (sortedSet.Add((score, member)))
                            addedOrChanged++;

                        this.UpdateSize(member);
                    }
                    // Update existing member
                    else
                    {
                        // Update new score if INCR flag is set
                        if ((options & SortedSetAddOption.INCR) == SortedSetAddOption.INCR)
                        {
                            score += scoreRef;
                            incrResult = score;
                        }

                        // No need for update
                        if (score == scoreRef)
                            continue;

                        // Don't update existing member if NX flag is set
                        // or if GT/LT flag is set and existing score is higher/lower than new score, respectively
                        if ((options & SortedSetAddOption.NX) == SortedSetAddOption.NX ||
                            ((options & SortedSetAddOption.GT) == SortedSetAddOption.GT && scoreRef > score) ||
                            ((options & SortedSetAddOption.LT) == SortedSetAddOption.LT && scoreRef < score)) continue;

                        // Remove old sorted set entry
                        var success = sortedSet.Remove((scoreRef, member));
                        Debug.Assert(success);

                        // Update the score and insert new sorted set entry
                        scoreRef = score;
                        success = sortedSet.Add((score, member));
                        Debug.Assert(success);

                        // If CH flag is set, add changed member to final count
                        if ((options & SortedSetAddOption.CH) == SortedSetAddOption.CH)
                            addedOrChanged++;
                    }
                }

                if ((options & SortedSetAddOption.INCR) == SortedSetAddOption.INCR)
                {
                    while (!RespWriteUtils.TryWriteDoubleBulkString(incrResult, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    while (!RespWriteUtils.TryWriteInt32(addedOrChanged, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRemove(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var value = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;

                if (!Remove(value))
                    continue;

                _output->result1++;
            }
        }

        private void SortedSetLength(byte* output)
        {
            // Check both objects
            Debug.Assert(Dictionary.Count == sortedSet.Count, "SortedSet object is not in sync.");
            ((ObjectOutputHeader*)output)->result1 = Dictionary.Count;
        }

        private void SortedSetScore(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZSCORE key member
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            var member = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

            ObjectOutputHeader outputHeader = default;
            try
            {
                if (!TryGetScore(member, out var score))
                {
                    while (!RespWriteUtils.TryWriteNull(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    var respVersion = input.arg1;

                    if (respVersion == 3)
                    {
                        while (!RespWriteUtils.TryWriteDoubleNumeric(score, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
                outputHeader.result1 = 1;
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetScores(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZMSCORE key member
            var count = input.parseState.Count;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;

            try
            {
                while (!RespWriteUtils.TryWriteArrayLength(count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                for (var i = 0; i < count; i++)
                {
                    var member = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;

                    if (!TryGetScore(member, out var score))
                    {
                        while (!RespWriteUtils.TryWriteNull(ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
                outputHeader.result1 = count;
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetCount(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            // Read min & max
            var minParamSpan = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            var maxParamSpan = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

            ObjectOutputHeader outputHeader = default;

            try
            {
                // Check if parameters are valid
                if (!TryParseParameter(minParamSpan, out var minValue, out var minExclusive) ||
                    !TryParseParameter(maxParamSpan, out var maxValue, out var maxExclusive))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                // get the elements within the score range and write the result
                var count = 0;
                if (sortedSet.Count > 0)
                {
                    foreach (var item in sortedSet.GetViewBetween((minValue, null), sortedSet.Max))
                    {
                        if (item.Item1 > maxValue || (maxExclusive && item.Item1 == maxValue)) break;
                        if (minExclusive && item.Item1 == minValue) continue;
                        count++;
                    }
                }

                while (!RespWriteUtils.TryWriteInt32(count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetIncrement(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZINCRBY key increment member
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;

            try
            {
                // Try to read increment value
                if (!input.parseState.TryGetDouble(0, out var incrValue))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                // Copy the member
                var member = input.parseState.GetArgSliceByRef(1).ReadOnlySpan.ToArray();

                // Avoid multiple hash calculations
                ref var scoreRef = ref CollectionsMarshal.GetValueRefOrAddDefault(Dictionary, member, out var exists);

                if (exists)
                {
                    // Remove old sorted set entry
                    sortedSet.Remove((scoreRef, member));

                    // Update the score and insert new sorted set entry
                    scoreRef += incrValue;
                    sortedSet.Add((scoreRef, member));
                }
                else
                {
                    scoreRef = incrValue;
                    sortedSet.Add((incrValue, member));

                    this.UpdateSize(member);
                }

                // Write the new score
                while (!RespWriteUtils.TryWriteDoubleBulkString(Dictionary[member], ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRange(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            //ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
            //ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
            //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
            var rangeOpts = (SortedSetRangeOpts)input.arg2;
            var count = input.parseState.Count;
            var respProtocolVersion = input.arg1;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            var currIdx = 0;

            ObjectOutputHeader _output = default;
            try
            {
                // Read min & max
                var minSpan = input.parseState.GetArgSliceByRef(currIdx++).ReadOnlySpan;
                var maxSpan = input.parseState.GetArgSliceByRef(currIdx++).ReadOnlySpan;

                // read the rest of the arguments
                ZRangeOptions options = new()
                {
                    ByScore = (rangeOpts & SortedSetRangeOpts.ByScore) != 0,
                    ByLex = (rangeOpts & SortedSetRangeOpts.ByLex) != 0,
                    Reverse = (rangeOpts & SortedSetRangeOpts.Reverse) != 0,
                    WithScores = (rangeOpts & SortedSetRangeOpts.WithScores) != 0 || (rangeOpts & SortedSetRangeOpts.Store) != 0
                };

                if (count > 2)
                {
                    while (currIdx < count)
                    {
                        var tokenSpan = input.parseState.GetArgSliceByRef(currIdx++).ReadOnlySpan;

                        if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("BYSCORE"u8))
                        {
                            options.ByScore = true;
                        }
                        else if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("BYLEX"u8))
                        {
                            options.ByLex = true;
                        }
                        else if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("REV"u8))
                        {
                            options.Reverse = true;
                        }
                        else if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("LIMIT"u8))
                        {
                            // Verify that there are at least 2 more tokens to read
                            if (input.parseState.Count - currIdx < 2)
                            {
                                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                return;
                            }

                            // Read the next two tokens
                            if (!input.parseState.TryGetInt(currIdx++, out var offset) ||
                                !input.parseState.TryGetInt(currIdx++, out var countLimit))
                            {
                                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                return;
                            }

                            options.Limit = (offset, countLimit);
                            options.ValidLimit = true;
                        }
                        else if (tokenSpan.EqualsUpperCaseSpanIgnoringCase("WITHSCORES"u8))
                        {
                            options.WithScores = true;
                        }
                    }
                }

                if (count >= 2 && ((!options.ByScore && !options.ByLex) || options.ByScore))
                {
                    if (!TryParseParameter(minSpan, out var minValue, out var minExclusive) ||
                        !TryParseParameter(maxSpan, out var maxValue, out var maxExclusive))
                    {
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                        return;
                    }

                    if (options.ByScore)
                    {
                        var scoredElements = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, options.WithScores, options.Reverse, options.ValidLimit, false, options.Limit);

                        WriteSortedSetResult(options.WithScores, scoredElements.Count, respProtocolVersion, scoredElements, ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {  // byIndex
                        int minIndex = (int)minValue, maxIndex = (int)maxValue;
                        if (options.ValidLimit)
                        {
                            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_LIMIT_NOT_SUPPORTED, ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            return;
                        }
                        else if (minValue > Dictionary.Count - 1)
                        {
                            // return empty list
                            while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            return;
                        }
                        else
                        {
                            //shift from the end of the set
                            if (minIndex < 0)
                            {
                                minIndex = Dictionary.Count + minIndex;
                            }
                            if (maxIndex < 0)
                            {
                                maxIndex = Dictionary.Count + maxIndex;
                            }
                            else if (maxIndex >= Dictionary.Count)
                            {
                                maxIndex = Dictionary.Count - 1;
                            }

                            // No elements to return if both indexes fall outside the range or min is higher than max
                            if ((minIndex < 0 && maxIndex < 0) || (minIndex > maxIndex))
                            {
                                while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                                return;
                            }
                            else
                            {
                                // Clamp minIndex to 0, if it is beyond the number of elements
                                minIndex = Math.Max(0, minIndex);

                                // calculate number of elements
                                var n = maxIndex - minIndex + 1;
                                var iterator = options.Reverse ? sortedSet.Reverse() : sortedSet;
                                iterator = iterator.Skip(minIndex).Take(n);

                                WriteSortedSetResult(options.WithScores, n, respProtocolVersion, iterator, ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            }
                        }
                    }
                }

                // by Lex
                if (count >= 2 && options.ByLex)
                {
                    var elementsInLex = GetElementsInRangeByLex(minSpan, maxSpan, options.Reverse, options.ValidLimit, false, out var errorCode, options.Limit);

                    if (errorCode == int.MaxValue)
                    {
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        WriteSortedSetResult(options.WithScores, elementsInLex.Count, respProtocolVersion, elementsInLex, ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        void WriteSortedSetResult(bool withScores, int count, int respProtocolVersion, IEnumerable<(double, byte[])> iterator, ref SpanByteAndMemory output, ref bool isMemory, ref byte* ptr, ref MemoryHandle ptrHandle, ref byte* curr, ref byte* end)
        {
            if (withScores && respProtocolVersion >= 3)
            {
                // write the size of the array reply
                while (!RespWriteUtils.TryWriteArrayLength(count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                foreach (var (score, element) in iterator)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(2, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    while (!RespWriteUtils.TryWriteBulkString(element, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    while (!RespWriteUtils.TryWriteDoubleNumeric(score, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
            }
            else
            {
                // write the size of the array reply
                while (!RespWriteUtils.TryWriteArrayLength(withScores ? count * 2 : count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                foreach (var (score, element) in iterator)
                {
                    while (!RespWriteUtils.TryWriteBulkString(element, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    if (withScores)
                    {
                        while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
        }

        private void SortedSetRemoveRangeByRank(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZREMRANGEBYRANK key start stop
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;

            try
            {
                if (!input.parseState.TryGetInt(0, out var start) ||
                    !input.parseState.TryGetInt(1, out var stop))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                if (start > Dictionary.Count - 1)
                    return;

                // Shift from the end of the set
                start = start < 0 ? Dictionary.Count + start : start;
                stop = stop < 0
                    ? Dictionary.Count + stop
                    : stop >= Dictionary.Count ? Dictionary.Count - 1 : stop;

                // Calculate number of elements
                var elementCount = stop - start + 1;

                // Copy the sorted set to avoid modified enumerator exception
                foreach (var item in sortedSet.Skip(start).Take(elementCount).ToList())
                {
                    Remove(item.Element, item.Score);
                }

                // Write the number of elements
                while (!RespWriteUtils.TryWriteInt32(elementCount, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRemoveRangeByScore(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            // ZREMRANGEBYSCORE key min max
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;

            try
            {
                // Read min and max
                var minParamBytes = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                var maxParamBytes = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                if (!TryParseParameter(minParamBytes, out var minValue, out var minExclusive) ||
                    !TryParseParameter(maxParamBytes, out var maxValue, out var maxExclusive))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                var elementCount = GetElementsInRangeByScore(minValue, maxValue, minExclusive, maxExclusive, false,
                    false, false, true).Count;

                // Write the number of elements
                while (!RespWriteUtils.TryWriteInt32(elementCount, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRandomMember(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var count = input.arg1 >> 2;
            var withScores = (input.arg1 & 1) == 1;
            var includedCount = ((input.arg1 >> 1) & 1) == 1;
            var seed = input.arg2;

            if (count > 0 && count > sortedSet.Count)
                count = sortedSet.Count;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader _output = default;
            try
            {
                // The count parameter can have a negative value, but the array length can't
                var arrayLength = Math.Abs(withScores ? count * 2 : count);
                if (arrayLength > 1 || (arrayLength == 1 && includedCount))
                {
                    while (!RespWriteUtils.TryWriteArrayLength(arrayLength, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }

                const int StackallocThreshold = 256;

                var indexCount = Math.Abs(count);

                var indices = indexCount <= StackallocThreshold ?
                    stackalloc int[StackallocThreshold].Slice(0, indexCount) : new int[indexCount];

                RandomUtils.PickRandomIndices(Dictionary.Count, indices, seed, count > 0);

                foreach (var item in indices)
                {
                    var (element, score) = Dictionary.ElementAt(item);

                    while (!RespWriteUtils.TryWriteBulkString(element, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    if (withScores)
                    {
                        while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }

                // Write count done into output footer
                _output.result1 = count;
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref _output, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        private void SortedSetRemoveOrCountRangeByLex(ref ObjectInput input, byte* output, SortedSetOperation op)
        {
            // ZREMRANGEBYLEX key min max
            // ZLEXCOUNT key min max
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            // Using minValue for partial execution detection
            _output->result1 = int.MinValue;

            var minParamBytes = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            var maxParamBytes = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

            var rem = GetElementsInRangeByLex(minParamBytes, maxParamBytes, false, false, op != SortedSetOperation.ZLEXCOUNT, out int errorCode);

            _output->result1 = errorCode;
            if (errorCode == 0)
                _output->result1 = rem.Count;
        }

        /// <summary>
        /// Gets the rank of a member of the sorted set
        /// in ascending or descending order
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="ascending"></param>
        private void SortedSetRank(ref ObjectInput input, ref SpanByteAndMemory output, bool ascending = true)
        {
            //ZRANK key member
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            var withScore = input.arg1 == 1;

            ObjectOutputHeader outputHeader = default;
            try
            {
                var member = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                if (!TryGetScore(member, out var score))
                {
                    while (!RespWriteUtils.TryWriteNull(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                }
                else
                {
                    var rank = 0;
                    foreach (var item in sortedSet)
                    {
                        if (member.SequenceEqual(item.Element))
                            break;
                        rank++;
                    }

                    if (!ascending)
                        rank = sortedSet.Count - rank - 1;

                    if (withScore)
                    {
                        while (!RespWriteUtils.TryWriteArrayLength(2, ref curr, end)) // Rank and score
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        while (!RespWriteUtils.TryWriteInt32(rank, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                        while (!RespWriteUtils.TryWriteDoubleBulkString(score, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteInt32(rank, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                }
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();

                output.Length = (int)(curr - ptr);
            }
        }

        /// <summary>
        /// Removes and returns the element with the highest or lowest score from the sorted set.
        /// </summary>
        /// <param name="popMaxScoreElement">If true, pops the element with the highest score; otherwise, pops the element with the lowest score.</param>
        /// <returns>A tuple containing the score and the element as a byte array.</returns>
        public (double Score, byte[] Element) PopMinOrMax(bool popMaxScoreElement = false)
        {
            if (sortedSet.Count == 0)
                return default;

            var element = popMaxScoreElement ? sortedSet.Max : sortedSet.Min;
            sortedSet.Remove(element);
            Dictionary.Remove(element.Element);
            this.UpdateSize(element.Element, false);

            return element;
        }

        /// <summary>
        /// Removes and returns up to COUNT members with the low or high score
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="op"></param>
        private void SortedSetPopMinOrMaxCount(ref ObjectInput input, ref SpanByteAndMemory output, SortedSetOperation op)
        {
            var count = input.arg1;
            var countDone = 0;

            if (sortedSet.Count < count)
                count = sortedSet.Count;

            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            ObjectOutputHeader outputHeader = default;

            try
            {
                while (!RespWriteUtils.TryWriteArrayLength(count * 2, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                while (count > 0)
                {
                    var max = op == SortedSetOperation.ZPOPMAX ? sortedSet.Max : sortedSet.Min;
                    sortedSet.Remove(max);
                    Dictionary.Remove(max.Element);

                    this.UpdateSize(max.Element, false);

                    while (!RespWriteUtils.TryWriteBulkString(max.Element, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    while (!RespWriteUtils.TryWriteDoubleBulkString(max.Score, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    countDone++;
                    count--;
                }

                outputHeader.result1 = countDone;
            }
            finally
            {
                while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                if (isMemory) ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        #region CommonMethods

        /// <summary>
        /// Gets the elements that belong to the Range using lexicographical order
        /// </summary>
        /// <param name="minParamByteArray"></param>
        /// <param name="maxParamByteArray"></param>
        /// <param name="doReverse">Perfom reverse order</param>
        /// <param name="validLimit">Use a limit offset count</param>
        /// <param name="remove">Remove elements</param>
        /// <param name="errorCode">errorCode</param>
        /// <param name="limit">offset and count values</param>
        /// <returns></returns>
        private List<(double Score, byte[] Element)> GetElementsInRangeByLex(
            ReadOnlySpan<byte> minParamByteArray,
            ReadOnlySpan<byte> maxParamByteArray,
            bool doReverse,
            bool validLimit,
            bool remove,
            out int errorCode,
            (int, int) limit = default)
        {
            var elementsInLex = new List<(double, byte[])>();

            // parse boundaries
            if (!TryParseLexParameter(minParamByteArray, out var minValueChars, out bool minValueExclusive) ||
                !TryParseLexParameter(maxParamByteArray, out var maxValueChars, out bool maxValueExclusive))
            {
                errorCode = int.MaxValue;
                return elementsInLex;
            }

            try
            {
                if (doReverse)
                {
                    var tmpMinValueChars = minValueChars;
                    minValueChars = maxValueChars;
                    maxValueChars = tmpMinValueChars;
                }

                var iterator = sortedSet.GetViewBetween((sortedSet.Min.Score, minValueChars.ToArray()), sortedSet.Max);

                // Copy to avoid the Invalid operation ex. when removing
                foreach (var item in iterator.ToList())
                {
                    var inRange = item.Element.AsSpan().SequenceCompareTo(minValueChars);
                    if (inRange < 0 || (inRange == 0 && minValueExclusive))
                        continue;

                    var outRange = maxValueChars.IsEmpty ? -1 : item.Element.AsSpan().SequenceCompareTo(maxValueChars);
                    if (outRange > 0 || (outRange == 0 && maxValueExclusive))
                        break;

                    if (remove)
                        Remove(item.Element, item.Score);
                    
                    elementsInLex.Add(item);
                }

                if (doReverse) elementsInLex.Reverse();

                if (validLimit)
                {
                    elementsInLex = [.. elementsInLex
                                        .Skip(limit.Item1 > 0 ? limit.Item1 : 0)
                                        .Take(limit.Item2 > 0 ? limit.Item2 : elementsInLex.Count)];
                }
            }
            catch (ArgumentException)
            {
                // this exception is thrown when the SortedSet is empty
                Debug.Assert(sortedSet.Count == 0);
            }

            errorCode = 0;
            return elementsInLex;
        }

        /// <summary>
        /// Gets a range of elements using by score filters, when
        /// rem flag is true, removes the elements in the range
        /// </summary>
        /// <param name="minValue"></param>
        /// <param name="maxValue"></param>
        /// <param name="minExclusive"></param>
        /// <param name="maxExclusive"></param>
        /// <param name="withScore"></param>
        /// <param name="doReverse"></param>
        /// <param name="validLimit"></param>
        /// <param name="remove"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        private List<(double Score, byte[] Element)> GetElementsInRangeByScore(double minValue, double maxValue, bool minExclusive, bool maxExclusive, bool withScore, bool doReverse, bool validLimit, bool remove, (int, int) limit = default)
        {
            if (doReverse)
            {
                (minValue, maxValue) = (maxValue, minValue);
            }

            List<(double Score, byte[] Element)> scoredElements = new();
            if (sortedSet.Max.Score < minValue)
            {
                return scoredElements;
            }

            foreach (var item in sortedSet.GetViewBetween((minValue, null), sortedSet.Max))
            {
                if (item.Score > maxValue || (maxExclusive && item.Score == maxValue)) break;
                if (minExclusive && item.Score == minValue) continue;
                scoredElements.Add(item);
            }
            if (doReverse) scoredElements.Reverse();
            if (validLimit)
            {
                scoredElements = [.. scoredElements
                                 .Skip(limit.Item1 > 0 ? limit.Item1 : 0)
                                 .Take(limit.Item2 > 0 ? limit.Item2 : scoredElements.Count)];
            }

            if (remove)
            {
                // Copy to avoid invalid operation exception when trying to mutate list while enumerating it
                foreach (var item in scoredElements.ToList())
                {
                    Remove(item.Element, item.Score);
                }
            }

            return scoredElements;
        }

        #endregion

        #region HelperMethods

        /// <summary>
        /// Helper method to parse parameters min and max
        /// in commands including +inf -inf
        /// </summary>
        private static bool TryParseParameter(ReadOnlySpan<byte> val, out double valueDouble, out bool exclusive)
        {
            exclusive = false;

            // adjust for exclusion
            if (val[0] == '(')
            {
                val = val.Slice(1);
                exclusive = true;
            }

            if (NumUtils.TryParse(val, out valueDouble))
            {
                return true;
            }

            var strVal = Encoding.ASCII.GetString(val);
            if (string.Equals("+inf", strVal, StringComparison.OrdinalIgnoreCase))
            {
                valueDouble = double.PositiveInfinity;
                exclusive = false;
                return true;
            }
            else if (string.Equals("-inf", strVal, StringComparison.OrdinalIgnoreCase))
            {
                valueDouble = double.NegativeInfinity;
                exclusive = false;
                return true;
            }
            return false;
        }

        /// <summary>
        /// Helper method to parse parameter when using Lexicographical ranges
        /// </summary>
        private static bool TryParseLexParameter(ReadOnlySpan<byte> val, out ReadOnlySpan<byte> limitChars, out bool limitExclusive)
        {
            limitChars = default;
            limitExclusive = false;

            switch (val[0])
            {
                case (byte)'+':
                case (byte)'-':
                    return true;
                case (byte)'[':
                    limitChars = val.Slice(1);
                    limitExclusive = false;
                    return true;
                case (byte)'(':
                    limitChars = val.Slice(1);
                    limitExclusive = true;
                    return true;
                default:
                    return false;
            }
        }

        #endregion
    }
}