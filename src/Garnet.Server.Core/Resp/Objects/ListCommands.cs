// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;
using Tsavorite;

namespace Garnet.Server;

internal sealed unsafe partial class RespServerSession
{
    /// <summary>
    /// Session counter of number of List entries(PUSH,POP etc.) partially done
    /// </summary>
    private int listItemsDoneCount;

    /// <summary>
    /// Session counter of number of List operations partially done
    /// </summary>
    private int listOpsCount;

    /// <summary>
    /// LPUSH key element[element...]
    /// RPUSH key element [element ...]
    /// </summary>
    private unsafe bool ListPush<TGarnetApi>(int count, byte* ptr, ListOperation lop, ref TGarnetApi storageApi)
                        where TGarnetApi : IGarnetApi
    {
        if (count < 2)
        {
            return AbortWithWrongNumberOfArguments(lop.ToString(), count);
        }

        // Get the key for List
        if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] sskey, ref ptr, recvBufferPtr + bytesRead))
            return false;

        if (NetworkSingleKeySlotVerify(sskey, false))
        {
            var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
            if (!DrainCommands(bufSpan, count))
                return false;
            return true;
        }

        // Prepare input
        var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

        // Save old values on buffer for possible revert
        ObjectInputHeader save = *inputPtr;

        int inputCount = count - 1;
        // Prepare length of header in input buffer
        int inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

        // Prepare header in input buffer
        inputPtr->header.type = GarnetObjectType.List;
        inputPtr->header.flags = 0;
        inputPtr->header.ListOp = lop;
        inputPtr->count = inputCount;
        inputPtr->done = listItemsDoneCount;

        var input = new ArgSlice((byte*)inputPtr, inputLength);

        ObjectOutputHeader output;
        output = default;

        GarnetStatus status = GarnetStatus.OK;

        if (lop == ListOperation.LPUSH || lop == ListOperation.LPUSHX)
            status = storageApi.ListLeftPush(sskey, input, out output);
        else
            status = storageApi.ListRightPush(sskey, input, out output);

        //restore input buffer
        *inputPtr = save;

        listItemsDoneCount += output.countDone;
        listOpsCount += output.opsDone;

        //return if command is only partially done
        if (output.countDone == int.MinValue && listOpsCount < inputCount)
            return false;

        // FIXME: Need to use ptr += output.bytesDone; instead of ReadLeftToken

        // Skip the element tokens on the input buffer
        int tokens = ReadLeftToken(count - 1, ref ptr);
        if (tokens < count - 1)
            return false;

        //write result to output
        while (!RespWriteUtils.WriteInteger(listItemsDoneCount, ref dcurr, dend))
            SendAndReset();

        //reset session counters
        listItemsDoneCount = listOpsCount = 0;

        // Move head
        readHead = (int)(ptr - recvBufferPtr);
        return true;
    }

    /// <summary>
    /// LPOP key [count]
    /// RPOP key [count]
    /// </summary>
    private unsafe bool ListPop<TGarnetApi>(int count, byte* ptr, ListOperation lop, ref TGarnetApi storageApi)
                        where TGarnetApi : IGarnetApi
    {
        if (count < 1)
        {
            return AbortWithWrongNumberOfArguments(lop.ToString(), count);
        }

        // Get the key for List
        if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] key, ref ptr, recvBufferPtr + bytesRead))
            return false;

        if (NetworkSingleKeySlotVerify(key, false))
        {
            var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
            if (!DrainCommands(bufSpan, count))
                return false;
            return true;
        }

        // Prepare input
        var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));
        int popCount = 1;

        // Save old values on buffer for possible revert
        ObjectInputHeader save = *inputPtr;

        if (count == 2)
        {
            // Read count
            if (!RespReadUtils.ReadIntWithLengthHeader(out popCount, ref ptr, recvBufferPtr + bytesRead))
                return false;
        }

        // Prepare GarnetObjectStore output
        var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

        // Prepare length of header in input buffer
        int inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

        // Prepare header in input buffer
        inputPtr->header.type = GarnetObjectType.List;
        inputPtr->header.flags = 0;
        inputPtr->header.ListOp = lop;
        inputPtr->done = 0;
        inputPtr->count = popCount;

        GarnetStatus statusOp = GarnetStatus.NOTFOUND;

        if (lop == ListOperation.LPOP)
            statusOp = storageApi.ListLeftPop(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);
        else
            statusOp = storageApi.ListRightPop(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

        // Reset input buffer
        *inputPtr = save;

        switch (statusOp)
        {
            case GarnetStatus.OK:
                //process output
                ObjectOutputHeader objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                ptr += objOutputHeader.bytesDone;
                break;
            case GarnetStatus.NOTFOUND:
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                    SendAndReset();
                break;
        }

        // Move input head
        readHead = (int)(ptr - recvBufferPtr);
        return true;
    }

    /// <summary>
    /// LLEN key
    /// Gets the length of the list stored at key.
    /// </summary>
    private unsafe bool ListLength<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
                        where TGarnetApi : IGarnetApi
    {
        if (count != 1)
        {
            return AbortWithWrongNumberOfArguments("LLEN", count);
        }
        else
        {
            // Get the key for List
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, true))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // save old values
            ObjectInputHeader save = *inputPtr;

            // Prepare length of header in input buffer
            int inputLength = (int)(recvBufferPtr + bytesRead - ptr) + sizeof(ObjectInputHeader);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.flags = 0;
            inputPtr->header.ListOp = ListOperation.LLEN;
            inputPtr->count = count;
            inputPtr->done = 0;

            GarnetStatus status = storageApi.ListLength(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

            //restore input buffer
            *inputPtr = save;

            if (status == GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                // Process output
                while (!RespWriteUtils.WriteInteger(output.countDone, ref dcurr, dend))
                    SendAndReset();
            }
        }

        // Move input head, write result to output
        readHead = (int)(ptr - recvBufferPtr);

        return true;
    }

    /// <summary>
    /// LTRIM key start stop
    /// Trim an existing list so it only contains the specified range of elements.
    /// </summary>
    private unsafe bool ListTrim<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
                        where TGarnetApi : IGarnetApi
    {
        if (count != 3)
        {
            return AbortWithWrongNumberOfArguments("LTRIM", count);
        }
        else
        {
            // Get the key for List
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            // Read the parameters(start and stop) from LTRIM
            if (!RespReadUtils.ReadIntWithLengthHeader(out int start, ref ptr, recvBufferPtr + bytesRead))
                return false;

            // Read the parameters(start and stop) from LTRIM
            if (!RespReadUtils.ReadIntWithLengthHeader(out int stop, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, false))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values on buffer for possible revert
            ObjectInputHeader save = *inputPtr;

            // Prepare length of header in input buffer
            int inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.flags = 0;
            inputPtr->header.ListOp = ListOperation.LTRIM;
            inputPtr->count = start;
            inputPtr->done = stop;

            GarnetStatus statusOp = storageApi.ListTrim(key, new ArgSlice((byte*)inputPtr, inputLength));

            //restore input buffer
            *inputPtr = save;

            //GarnetStatus.OK or NOTFOUND have same result
            // no need to process output, just send OK
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
        }
        // Move input head, write result to output
        readHead = (int)(ptr - recvBufferPtr);
        return true;
    }

    /// <summary>
    /// Gets the specified elements of the list stored at key.
    /// LRANGE key start stop
    /// </summary>
    private unsafe bool ListRange<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
         where TGarnetApi : IGarnetApi
    {
        if (count != 3)
        {
            return AbortWithWrongNumberOfArguments("LRANGE", count);
        }
        else
        {
            // Get the key for List
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            // Read count start and stop params for LRANGE
            if (!RespReadUtils.ReadIntWithLengthHeader(out int start, ref ptr, recvBufferPtr + bytesRead))
                return false;
            if (!RespReadUtils.ReadIntWithLengthHeader(out int end, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, true))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values on buffer for possible revert
            ObjectInputHeader save = *inputPtr;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            // Prepare length of header in input buffer
            int inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.flags = 0;
            inputPtr->header.ListOp = ListOperation.LRANGE;
            inputPtr->count = start;
            inputPtr->done = end;

            GarnetStatus statusOp = storageApi.ListRange(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            // Reset input buffer
            *inputPtr = save;

            switch (statusOp)
            {
                case GarnetStatus.OK:
                    //process output
                    ObjectOutputHeader objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    ptr += objOutputHeader.bytesDone;
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_EMPTYLIST, ref dcurr, dend))
                        SendAndReset();
                    break;
            }
        }
        // Move input head, write result to output
        readHead = (int)(ptr - recvBufferPtr);
        return true;
    }

    /// <summary>
    /// Returns the element at index.
    /// LINDEX key index
    /// </summary>
    private unsafe bool ListIndex<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
         where TGarnetApi : IGarnetApi
    {
        if (count != 2)
        {
            return AbortWithWrongNumberOfArguments("LINDEX", count);
        }
        else
        {
            // Get the key for List
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            // Read index param
            if (!RespReadUtils.ReadIntWithLengthHeader(out int index, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, true))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values for possible revert
            ObjectInputHeader save = *inputPtr;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            int inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.flags = 0;
            inputPtr->header.ListOp = ListOperation.LINDEX;
            inputPtr->count = index;
            inputPtr->done = 0;

            GarnetStatus statusOp = storageApi.ListIndex(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            //restore input
            *inputPtr = save;

            ReadOnlySpan<byte> error = CmdStrings.RESP_ERRNOTFOUND;

            switch (statusOp)
            {
                case GarnetStatus.OK:
                    //process output
                    ObjectOutputHeader objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    ptr += objOutputHeader.bytesDone;
                    if (objOutputHeader.opsDone != -1)
                        error = default;
                    break;
            }

            if (error != default)
            {
                while (!RespWriteUtils.WriteDirect(error, ref dcurr, dend))
                    SendAndReset();
            }
        }

        // Move input head, write result to output
        readHead = (int)(ptr - recvBufferPtr);
        return true;
    }

    /// <summary>
    /// Inserts a new element in the list stored at key either before or after a value pivot
    /// LINSERT key BEFORE|AFTER pivot element
    /// </summary>
    private unsafe bool ListInsert<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
         where TGarnetApi : IGarnetApi
    {
        if (count != 4)
        {
            return AbortWithWrongNumberOfArguments("LINSERT", count);
        }
        else
        {
            // Get the key for List
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, false))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values
            ObjectInputHeader save = *inputPtr;

            // Prepare length of header in input buffer
            int inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.flags = 0;
            inputPtr->header.ListOp = ListOperation.LINSERT;
            inputPtr->done = 0;
            inputPtr->count = 0;

            GarnetStatus statusOp = storageApi.ListInsert(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);

            //restore input buffer
            *inputPtr = save;

            switch (statusOp)
            {
                case GarnetStatus.OK:
                    //TODO: validation for different object type, pending to review
                    if (output.countDone == 0 && output.countDone == 0 && output.bytesDone == 0)
                    {
                        while (!RespWriteUtils.WriteError("ERR wrong key type used in LINSERT command."u8, ref dcurr, dend))
                            SendAndReset();
                    }
                    //check for partial execution
                    if (output.countDone == int.MinValue)
                        return false;
                    //process output
                    ptr += output.bytesDone;
                    while (!RespWriteUtils.WriteInteger(output.opsDone, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    int tokens = ReadLeftToken(count - 1, ref ptr);
                    if (tokens < count - 1)
                        return false;
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
            }
        }

        // Move input head, write result to output
        readHead = (int)(ptr - recvBufferPtr);
        return true;
    }

    /// <summary>
    /// LREM key count element
    /// </summary>
    private unsafe bool ListRemove<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
          where TGarnetApi : IGarnetApi
    {
        // if params are missing return error
        if (count != 3)
        {
            return AbortWithWrongNumberOfArguments("LREM", count);
        }
        else
        {
            // Get the key for List
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            // Get count parameter
            if (!RespReadUtils.ReadIntWithLengthHeader(out int nCount, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, false))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values
            ObjectInputHeader save = *inputPtr;

            // Prepare length of header in input buffer
            int inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.flags = 0;
            inputPtr->header.ListOp = ListOperation.LREM;
            inputPtr->count = nCount;
            inputPtr->done = 0;

            GarnetStatus statusOp = storageApi.ListRemove(key, new ArgSlice((byte*)inputPtr, inputLength), out ObjectOutputHeader output);
            //restore input buffer
            *inputPtr = save;

            switch (statusOp)
            {
                case GarnetStatus.OK:
                    //check for partial execution
                    if (output.countDone == int.MinValue)
                        return false;
                    //process output
                    ptr += output.bytesDone;
                    while (!RespWriteUtils.WriteInteger(output.opsDone, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    int tokens = ReadLeftToken(count - 2, ref ptr);
                    if (tokens < count - 2)
                        return false;
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                        SendAndReset();
                    break;
            }
        }
        // Move input head, write result to output
        readHead = (int)(ptr - recvBufferPtr);
        return true;
    }


    /// <summary>
    /// LMOVE source destination [LEFT | RIGHT] [LEFT | RIGHT]
    /// </summary>
    private unsafe bool ListMove<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
         where TGarnetApi : IGarnetApi
    {
        bool result = false;

        if (count != 4)
        {
            return AbortWithWrongNumberOfArguments("LMOVE", count);
        }
        else
        {
            ArgSlice sourceKey = default, destinationKey = default, param1 = default, param2 = default;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref sourceKey.ptr, ref sourceKey.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref destinationKey.ptr, ref destinationKey.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref param1.ptr, ref param1.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref param2.ptr, ref param2.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            OperationDirection sourceDirection = GetOperationDirection(param1);
            OperationDirection destinationDirection = GetOperationDirection(param2);
            if (sourceDirection == OperationDirection.Unknown || destinationDirection == OperationDirection.Unknown)
            {
                return AbortWithErrorMessage(count, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            }

            result = ListMove(count, sourceKey, destinationKey, sourceDirection, destinationDirection, out byte[] node, ref storageApi);
            if (node != null)
            {
                while (!RespWriteUtils.WriteBulkString(node, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteNull(ref dcurr, dend))
                    SendAndReset();
            }
        }

        // Move input head, write result to output
        readHead = (int)(ptr - recvBufferPtr);
        return result;
    }

    /// <summary>
    /// RPOPLPUSH source destination
    /// </summary>
    private unsafe bool ListRightPopLeftPush<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
        where TGarnetApi : IGarnetApi
    {
        bool result = false;

        if (count != 2)
        {
            return AbortWithWrongNumberOfArguments("RPOPLPUSH", count);
        }
        else
        {
            ArgSlice sourceKey = default, destinationKey = default;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref sourceKey.ptr, ref sourceKey.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref destinationKey.ptr, ref destinationKey.length, ref ptr, recvBufferPtr + bytesRead))
                return false;

            result = ListMove(count, sourceKey, destinationKey, OperationDirection.Right, OperationDirection.Left, out byte[] node, ref storageApi);

            if (node != null)
            {
                while (!RespWriteUtils.WriteBulkString(node, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteNull(ref dcurr, dend))
                    SendAndReset();
            }
        }

        // update read pointers
        readHead = (int)(ptr - recvBufferPtr);
        return result;
    }

    /// <summary>
    /// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
    /// RPOPLPUSH source destination
    /// </summary>
    /// <param name="count">Number of tokens in input</param>
    private unsafe bool ListMove<TGarnetApi>(int count, ArgSlice sourceKey, ArgSlice destinationKey, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] node, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
    {
        ArgSlice[] keys = new ArgSlice[2] { sourceKey, destinationKey };
        node = null;
        if (NetworkKeyArraySlotVerify(ref keys, false))
        {
            // check for non crosslot error
            var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
            if (!DrainCommands(bufSpan, count))
            {
                return false;
            }
            return true;
        }

        return storageApi.ListMove(sourceKey, destinationKey, sourceDirection, destinationDirection, out node);
    }

    /// <summary>
    /// Sets the list element at index to element
    /// LSET key index element
    /// </summary>
    public unsafe bool ListSet<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
        where TGarnetApi : IGarnetApi
    {
        if (count != 3)
        {
            return AbortWithWrongNumberOfArguments("LSET", count);
        }
        else
        {
            // Get the key for List
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] key, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(key, true))
            {
                var bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }

            // Prepare input
            var inputPtr = (ObjectInputHeader*)(ptr - sizeof(ObjectInputHeader));

            // Save old values for possible revert
            ObjectInputHeader save = *inputPtr;

            int inputLength = (int)(recvBufferPtr + bytesRead - (byte*)inputPtr);

            // Prepare header in input buffer
            inputPtr->header.type = GarnetObjectType.List;
            inputPtr->header.flags = 0;
            inputPtr->header.ListOp = ListOperation.LSET;
            inputPtr->count = 0;
            inputPtr->done = 0;

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };

            GarnetStatus statusOp = storageApi.ListSet(key, new ArgSlice((byte*)inputPtr, inputLength), ref outputFooter);

            //restore input
            *inputPtr = save;

            switch (statusOp)
            {
                case GarnetStatus.OK:
                    //process output
                    ObjectOutputHeader objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    ptr += objOutputHeader.bytesDone;
                    break;
            }
        }

        // Move input head, write result to output
        readHead = (int)(ptr - recvBufferPtr);
        return true;
    }
}