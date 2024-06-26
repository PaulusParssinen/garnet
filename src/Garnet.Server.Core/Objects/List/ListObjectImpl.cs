﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using Garnet.Common;
using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// List - RESP specific operations
/// </summary>
public unsafe partial class ListObject : IGarnetObject
{

    private void ListRemove(byte* input, int length, byte* output)
    {
        var _input = (ObjectInputHeader*)input;
        byte* startptr = input + sizeof(ObjectInputHeader);
        byte* ptr = startptr;
        byte* end = input + length;

        var _output = (ObjectOutputHeader*)output;
        *_output = default;

        //indicates partial execution
        _output->countDone = int.MinValue;

        // get the source string to remove
        if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] item, ref ptr, end))
            return;

        int count = _input->count;
        int rem_count = 0;
        _output->countDone = 0;

        //remove all equals to item
        if (count == 0)
        {
            int elements = list.Count;
            list.Where(i => i.SequenceEqual(item)).ToList().ForEach(i => { list.Remove(i); UpdateSize(i, false); });
            rem_count = elements - list.Count;
        }
        else
        {
            while (rem_count < Math.Abs(count) && list.Count > 0)
            {
                byte[] node = count > 0 ? list.FirstOrDefault(i => i.SequenceEqual(item)) : list.LastOrDefault(i => i.SequenceEqual(item));
                if (node != null)
                {
                    list.Remove(node);
                    UpdateSize(node, false);
                    rem_count++;
                }
                else
                {
                    break;
                }
            }
        }
        _output->bytesDone = (int)(ptr - startptr);
        _output->opsDone = rem_count;
    }

    private void ListInsert(byte* input, int length, byte* output)
    {
        var _input = (ObjectInputHeader*)input;
        byte* startptr = input + sizeof(ObjectInputHeader);
        byte* ptr = startptr;
        byte* end = input + length;
        LinkedListNode<byte[]> current = null;

        var _output = (ObjectOutputHeader*)output;
        *_output = default;

        //indicates partial execution
        _output->countDone = int.MinValue;

        if (list.Count > 0)
        {
            // figure out where to insert BEFORE or AFTER
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] position, ref ptr, end))
                return;

            // get the source string
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] pivot, ref ptr, end))
                return;

            // get the string to INSERT into the list
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] insertitem, ref ptr, end))
                return;

            bool fBefore = CmdStrings.BEFORE.SequenceEqual(position);

            // find the first ocurrence of the pivot element
            current = list.Nodes().DefaultIfEmpty(null).FirstOrDefault(i => i.Value.SequenceEqual(pivot));
            LinkedListNode<byte[]> newNode = current != default ? (fBefore ? list.AddBefore(current, insertitem) : list.AddAfter(current, insertitem)) : default;
            if (current != null)
                UpdateSize(insertitem);
            _output->opsDone = current != default ? list.Count : -1;
            _output->countDone = _output->opsDone;
        }
        // Write bytes parsed from input and count done, into output footer
        _output->bytesDone = (int)(ptr - startptr);
    }

    private void ListIndex(byte* input, ref SpanByteAndMemory output)
    {
        var _input = (ObjectInputHeader*)input;

        bool isMemory = false;
        MemoryHandle ptrHandle = default;
        byte* ptr = output.SpanByte.ToPointer();

        byte* curr = ptr;
        byte* end = curr + output.Length;
        byte[] item = default;

        ObjectOutputHeader _output = default;
        _output.opsDone = -1;
        try
        {
            int index = _input->count < 0 ? list.Count + _input->count : _input->count;
            item = list.ElementAtOrDefault(index);
            if (item != default)
            {
                while (!RespWriteUtils.WriteBulkString(item, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                _output.opsDone = 1;
            }
        }
        finally
        {
            _output.countDone = _output.opsDone;
            _output.bytesDone = 0;

            while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

            if (isMemory) ptrHandle.Dispose();
            output.Length = (int)(curr - ptr);
        }
    }

    private void ListRange(byte* input, ref SpanByteAndMemory output)
    {
        var _input = (ObjectInputHeader*)input;

        bool isMemory = false;
        MemoryHandle ptrHandle = default;
        byte* ptr = output.SpanByte.ToPointer();

        byte* curr = ptr;
        byte* end = curr + output.Length;

        ObjectOutputHeader _output = default;
        try
        {
            if (0 == list.Count)
            {
                // write empty list
                while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            else
            {
                int start = _input->count < 0 ? list.Count + _input->count : _input->count;
                if (start < 0) start = 0;

                int stop = _input->done < 0 ? list.Count + _input->done : _input->done;
                if (stop < 0) stop = 0;
                if (stop >= list.Count) stop = list.Count - 1;

                if (start > stop || 0 == list.Count)
                {
                    while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    _output.opsDone = 0;
                }
                else
                {
                    int count = stop - start + 1;
                    while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    int i = -1;
                    foreach (byte[] bytes in list)
                    {
                        i++;
                        if (i < start)
                            continue;
                        if (i > stop)
                            break;
                        while (!RespWriteUtils.WriteBulkString(bytes, ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    _output.opsDone = count;
                }
            }
            //updating output
            _output.bytesDone = 0; // no reads done 
            _output.countDone = _output.opsDone;
        }
        finally
        {
            while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

            if (isMemory) ptrHandle.Dispose();
            output.Length = (int)(curr - ptr);
        }
    }

    private void ListTrim(byte* input, byte* output)
    {
        var _input = (ObjectInputHeader*)input;
        var _output = (ObjectOutputHeader*)output;

        if (list.Count > 0)
        {
            int start = _input->count < 0 ? list.Count + _input->count : _input->count;
            if (start < -(list.Count - 1) || start >= list.Count) start = list.Count - 1;

            int end = _input->done < 0 ? list.Count + _input->done : _input->done;
            if (end < -(list.Count - 1) || end >= list.Count) end = list.Count - 1;

            Debug.Assert(end - start <= list.Count);
            if (start > end)
            {
                _output->opsDone = list.Count;
                list.Clear();
            }
            else
            {
                // Only  the first end+1 elements will remain
                if (start == 0)
                {
                    int numDeletes = list.Count - (end + 1);
                    for (int i = 0; i < numDeletes; i++)
                    {
                        byte[] _value = list.Last.Value;
                        list.RemoveLast();
                        UpdateSize(_value, false);
                    }
                    _output->opsDone = numDeletes;
                }
                else
                {
                    int i = 0;
                    IList<byte[]> readOnly = new List<byte[]>(list).AsReadOnly();
                    foreach (byte[] node in readOnly)
                    {
                        if (!(i >= start && i <= end))
                        {
                            list.Remove(node);
                            UpdateSize(node, false);
                        }
                        i++;
                    }
                    _output->opsDone = i;
                }
            }
            _output->bytesDone = 0;
            _output->countDone = _output->opsDone;
        }
    }

    private void ListLength(byte* input, byte* output)
    {
        ((ObjectOutputHeader*)output)->countDone = list.Count;
    }

    private void ListPush(byte* input, int length, byte* output, bool fAddAtHead)
    {
        var _input = (ObjectInputHeader*)input;
        var _output = (ObjectOutputHeader*)output;

        int count = _input->count;
        *_output = default;

        byte* startptr = input + sizeof(ObjectInputHeader);
        byte* ptr = startptr;
        byte* end = input + length;

        //this value is used in the validations for partial execution
        _output->countDone = int.MinValue;

        for (int c = 0; c < count; c++)
        {
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] value, ref ptr, end))
                return;

            if (c < _input->done)
                continue;

            //Add the value to the top of the list
            if (fAddAtHead)
                list.AddFirst(value);
            else
                list.AddLast(value);

            UpdateSize(value);
            _output->countDone = list.Count;
            _output->opsDone++;
            _output->bytesDone = (int)(ptr - startptr);
        }
    }

    private void ListPop(byte* input, ref SpanByteAndMemory output, bool fDelAtHead)
    {
        var _input = (ObjectInputHeader*)input;
        int count = _input->count; // for multiple elements

        byte* input_startptr = input + sizeof(ObjectInputHeader);
        byte* input_currptr = input_startptr;

        if (list.Count < count)
            count = list.Count;

        int countDone = 0;

        bool isMemory = false;
        MemoryHandle ptrHandle = default;
        byte* ptr = output.SpanByte.ToPointer();

        byte* curr = ptr;
        byte* end = curr + output.Length;

        ObjectOutputHeader _output = default;
        try
        {
            if (list.Count == 0)
            {
                while (!RespWriteUtils.WriteNull(ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                count = 0;
            }
            else if (count > 1)
            {
                while (!RespWriteUtils.WriteArrayLength(count, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }

            while (count > 0 && list.Any())
            {
                LinkedListNode<byte[]> node = null;
                if (fDelAtHead)
                {
                    node = list.First;
                    list.RemoveFirst();
                }
                else
                {
                    node = list.Last;
                    list.RemoveLast();
                }

                UpdateSize(node.Value, false);
                while (!RespWriteUtils.WriteBulkString(node.Value, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                count--;
                countDone++;
            }

            // Write bytes parsed from input and count done, into output footer
            _output.bytesDone = (int)(input_currptr - input_startptr);
            _output.countDone = countDone;
            _output.opsDone = countDone;
        }
        finally
        {
            while (!RespWriteUtils.WriteDirect(ref _output, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

            if (isMemory) ptrHandle.Dispose();
            output.Length = (int)(curr - ptr);
        }
    }

    private void ListSet(byte* input, int length, ref SpanByteAndMemory output)
    {
        bool isMemory = false;
        MemoryHandle ptrHandle = default;
        byte* output_startptr = output.SpanByte.ToPointer();
        byte* output_currptr = output_startptr;
        byte* output_end = output_currptr + output.Length;

        ObjectOutputHeader _output = default;

        try
        {
            if (list.Count == 0)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY, ref output_currptr, output_end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                return;
            }

            byte* input_startptr = input + sizeof(ObjectInputHeader);
            byte* input_currptr = input_startptr;
            byte* input_end = input + length;

            byte* indexParam = default;
            int indexParamSize = 0;

            // index
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref indexParam, ref indexParamSize, ref input_currptr, input_end))
                return;

            if (NumUtils.TryBytesToInt(indexParam, indexParamSize, out int index) == false)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref output_currptr, output_end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                return;
            }

            index = index < 0 ? list.Count + index : index;

            if (index > list.Count - 1 || index < 0)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_INDEX_OUT_RANGE, ref output_currptr, output_end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);
                return;
            }

            // element
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] element, ref input_currptr, input_end))
                return;

            LinkedListNode<byte[]> targetNode = index == 0 ? list.First
                : (index == list.Count - 1 ? list.Last
                    : list.Nodes().ElementAtOrDefault(index));

            UpdateSize(targetNode.Value, false);
            targetNode.Value = element;
            UpdateSize(targetNode.Value);

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref output_currptr, output_end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);

            // Write bytes parsed from input and count done, into output footer
            _output.bytesDone = (int)(input_currptr - input_startptr);
            _output.countDone = 1;
            _output.opsDone = 1;
        }
        finally
        {
            while (!RespWriteUtils.WriteDirect(ref _output, ref output_currptr, output_end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref output_startptr, ref ptrHandle, ref output_currptr, ref output_end);

            if (isMemory) ptrHandle.Dispose();
            output.Length = (int)(output_currptr - output_startptr);
        }
    }
}