// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Text;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Garnet.Common;
using Tsavorite;

namespace Garnet.Server;

internal sealed partial class StorageSession : IDisposable
{

    /// <summary>
    /// Adds the specified member and score to the sorted set stored at key.
    /// </summary>
    public unsafe GarnetStatus SortedSetAdd<TObjectContext>(ArgSlice key, ArgSlice score, ArgSlice member, out int zaddCount, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        zaddCount = 0;
        if (key.Length == 0)
            return GarnetStatus.OK;

        ArgSlice input = ScratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, score, member);

        // Prepare header in input buffer
        var rmwInput = (ObjectInputHeader*)input.ptr;
        rmwInput->header.type = GarnetObjectType.SortedSet;
        rmwInput->header.flags = 0;
        rmwInput->header.SortedSetOp = SortedSetOperation.ZADD;
        rmwInput->count = 1;
        rmwInput->done = 0;

        RMWObjectStoreOperation(key.ToArray(), input, out ObjectOutputHeader output, ref objectStoreContext);

        zaddCount = output.opsDone;
        return GarnetStatus.OK;
    }

    /// <summary>
    /// Adds all the specified members with the specified scores to the sorted set stored at key.
    /// Current members get the score updated and reordered.
    /// </summary>
    public unsafe GarnetStatus SortedSetAdd<TObjectContext>(ArgSlice key, (ArgSlice score, ArgSlice member)[] inputs, out int zaddCount, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        zaddCount = 0;

        if (inputs.Length == 0 || key.Length == 0)
            return GarnetStatus.OK;

        // Prepare header in buffer
        var rmwInput = (ObjectInputHeader*)ScratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
        rmwInput->header.type = GarnetObjectType.SortedSet;
        rmwInput->header.flags = 0;
        rmwInput->header.SortedSetOp = SortedSetOperation.ZADD;
        rmwInput->count = inputs.Length;
        rmwInput->done = 0;

        int inputLength = sizeof(ObjectInputHeader);
        foreach ((ArgSlice score, ArgSlice member) in inputs)
        {
            ArgSlice tmp = ScratchBufferManager.FormatScratchAsResp(0, score, member);
            inputLength += tmp.Length;
        }
        ArgSlice input = ScratchBufferManager.GetSliceFromTail(inputLength);

        RMWObjectStoreOperation(key.ToArray(), input, out ObjectOutputHeader output, ref objectStoreContext);

        zaddCount = output.opsDone;
        return GarnetStatus.OK;
    }

    /// <summary>
    /// Removes the specified member from the sorted set stored at key.
    /// Non existing members are ignored.
    /// </summary>
    public unsafe GarnetStatus SortedSetRemove<TObjectContext>(byte[] key, ArgSlice member, out int zremCount, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        zremCount = 0;

        if (key.Length == 0)
            return GarnetStatus.OK;

        ArgSlice _inputSlice = ScratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, member);

        // Prepare header in input buffer
        var rmwInput = (ObjectInputHeader*)_inputSlice.ptr;
        rmwInput->header.type = GarnetObjectType.SortedSet;
        rmwInput->header.flags = 0;
        rmwInput->header.SortedSetOp = SortedSetOperation.ZREM;
        rmwInput->count = 1;
        rmwInput->done = 0;

        RMWObjectStoreOperation(key, _inputSlice, out ObjectOutputHeader output, ref objectStoreContext);

        zremCount = output.opsDone;
        return GarnetStatus.OK;
    }

    /// <summary>
    /// Removes the specified members from the sorted set stored at key.
    /// Non existing members are ignored.
    /// </summary>
    public unsafe GarnetStatus SortedSetRemove<TObjectContext>(byte[] key, ArgSlice[] members, out int zremCount, ref TObjectContext objectStoreContext)
       where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        zremCount = 0;

        if (key.Length == 0 || members.Length == 0)
            return GarnetStatus.OK;

        // Prepare header in input buffer
        var rmwInput = (ObjectInputHeader*)ScratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
        rmwInput->header.type = GarnetObjectType.SortedSet;
        rmwInput->header.flags = 0;
        rmwInput->header.SortedSetOp = SortedSetOperation.ZREM;
        rmwInput->count = members.Length;
        rmwInput->done = 0;

        int inputLength = sizeof(ObjectInputHeader);
        foreach (ArgSlice member in members)
        {
            ArgSlice tmp = ScratchBufferManager.FormatScratchAsResp(0, member);
            inputLength += tmp.Length;
        }
        ArgSlice input = ScratchBufferManager.GetSliceFromTail(inputLength);

        RMWObjectStoreOperation(key, input, out ObjectOutputHeader output, ref objectStoreContext);

        zremCount = output.opsDone;
        return GarnetStatus.OK;
    }

    /// <summary>
    /// Removes all elements in the range specified by min and max, having the same score.
    /// </summary>
    public unsafe GarnetStatus SortedSetRemoveRangeByLex<TObjectContext>(ArgSlice key, string min, string max, out int countRemoved, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        countRemoved = 0;

        if (key.Length == 0)
            return GarnetStatus.OK;

        byte[] minBytes = Encoding.ASCII.GetBytes(min);
        byte[] maxBytes = Encoding.ASCII.GetBytes(max);

        fixed (byte* ptr = minBytes)
        {
            fixed (byte* ptr2 = maxBytes)
            {
                var minArgSlice = new ArgSlice(ptr, minBytes.Length);
                var maxArgSlice = new ArgSlice(ptr2, maxBytes.Length);
                ArgSlice _inputSlice = ScratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, minArgSlice, maxArgSlice);

                // Prepare header in input buffer
                var rmwInput = (ObjectInputHeader*)_inputSlice.ptr;
                rmwInput->header.type = GarnetObjectType.SortedSet;
                rmwInput->header.flags = 0;
                rmwInput->header.SortedSetOp = SortedSetOperation.ZREMRANGEBYLEX;
                rmwInput->count = 3;
                rmwInput->done = 0;

                RMWObjectStoreOperation(key.ToArray(), _inputSlice, out ObjectOutputHeader output, ref objectStoreContext);
                countRemoved = output.opsDone;
            }
        }

        return GarnetStatus.OK;
    }

    /// <summary>
    /// Removes all elements that have a score in the range specified by min and max.
    /// </summary>
    public unsafe GarnetStatus SortedSetRemoveRangeByScore<TObjectContext>(ArgSlice key, string min, string max, out int countRemoved, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        countRemoved = 0;

        if (key.Length == 0)
            return GarnetStatus.OK;

        byte[] minBytes = Encoding.ASCII.GetBytes(min);
        byte[] maxBytes = Encoding.ASCII.GetBytes(max);

        fixed (byte* ptr = minBytes)
        {
            fixed (byte* ptr2 = maxBytes)
            {
                var minArgSlice = new ArgSlice(ptr, minBytes.Length);
                var maxArgSlice = new ArgSlice(ptr2, maxBytes.Length);
                ArgSlice _inputSlice = ScratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, minArgSlice, maxArgSlice);

                // Prepare header in input buffer
                var rmwInput = (ObjectInputHeader*)_inputSlice.ptr;
                rmwInput->header.type = GarnetObjectType.SortedSet;
                rmwInput->header.flags = 0;
                rmwInput->header.SortedSetOp = SortedSetOperation.ZREMRANGEBYSCORE;
                rmwInput->count = 3;
                rmwInput->done = 0;

                RMWObjectStoreOperation(key.ToArray(), _inputSlice, out ObjectOutputHeader output, ref objectStoreContext);
                countRemoved = output.opsDone;
            }
        }

        return GarnetStatus.OK;
    }

    /// <summary>
    /// Removes all elements with the index in the range specified by start and stop.
    /// </summary>
    public unsafe GarnetStatus SortedSetRemoveRangeByRank<TObjectContext>(ArgSlice key, int start, int stop, out int countRemoved, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        countRemoved = 0;

        if (key.Length == 0)
            return GarnetStatus.OK;

        byte[] startBytes = Encoding.ASCII.GetBytes(start.ToString());
        byte[] stopBytes = Encoding.ASCII.GetBytes(stop.ToString());

        fixed (byte* ptr = startBytes)
        {
            fixed (byte* ptr2 = stopBytes)
            {
                var startArgSlice = new ArgSlice(ptr, startBytes.Length);
                var stopArgSlice = new ArgSlice(ptr2, stopBytes.Length);
                ArgSlice _inputSlice = ScratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, startArgSlice, stopArgSlice);

                // Prepare header in input buffer
                var rmwInput = (ObjectInputHeader*)_inputSlice.ptr;
                rmwInput->header.type = GarnetObjectType.SortedSet;
                rmwInput->header.flags = 0;
                rmwInput->header.SortedSetOp = SortedSetOperation.ZREMRANGEBYRANK;
                rmwInput->count = 3;
                rmwInput->done = 0;

                RMWObjectStoreOperation(key.ToArray(), _inputSlice, out ObjectOutputHeader output, ref objectStoreContext);
                countRemoved = output.opsDone;
            }
        }

        return GarnetStatus.OK;
    }

    /// <summary>
    /// Removes and returns up to count members with the highest or lowest scores in the sorted set stored at key.
    /// </summary>
    /// <param name="lowScoresFirst">When true return the lowest scores, otherwise the highest.</param>
    public unsafe GarnetStatus SortedSetPop<TObjectContext>(ArgSlice key, int count, bool lowScoresFirst, out (ArgSlice score, ArgSlice member)[] pairs, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        pairs = default;
        if (key.Length == 0)
            return GarnetStatus.OK;

        // Prepare header in input buffer
        ArgSlice input = ScratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);

        var inputPtr = (ObjectInputHeader*)input.ptr;
        inputPtr->header.type = GarnetObjectType.SortedSet;
        inputPtr->header.flags = 0;
        inputPtr->header.SortedSetOp = lowScoresFirst ? SortedSetOperation.ZPOPMIN : SortedSetOperation.ZPOPMAX;
        inputPtr->count = count;
        inputPtr->done = 0;

        var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

        GarnetStatus status = RMWObjectStoreOperationWithOutput(key.ToArray(), input, ref objectStoreContext, ref outputFooter);

        //process output
        //if (status == GarnetStatus.OK)
        ArgSlice[] npairs = ProcessRespArrayOutput(outputFooter, out string error);

        return status;
    }

    /// <summary>
    /// Increments the score of member in the sorted set stored at key by increment.
    /// Returns the new score of member.
    /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
    /// </summary>
    public unsafe GarnetStatus SortedSetIncrement<TObjectContext>(ArgSlice key, double increment, ArgSlice member, out double newScore, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        newScore = 0;

        if (key.Length == 0)
            return GarnetStatus.OK;

        byte[] incrementBytes = Encoding.ASCII.GetBytes(increment.ToString(CultureInfo.InvariantCulture));

        fixed (byte* ptr = incrementBytes)
        {
            var incrementArgSlice = new ArgSlice(ptr, incrementBytes.Length);
            ArgSlice _inputSlice = ScratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, incrementArgSlice, member);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)_inputSlice.ptr;
            rmwInput->header.type = GarnetObjectType.SortedSet;
            rmwInput->header.flags = 0;
            rmwInput->header.SortedSetOp = SortedSetOperation.ZINCRBY;
            rmwInput->count = 3;
            rmwInput->done = 0;

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            GarnetStatus status = RMWObjectStoreOperationWithOutput(key.ToArray(), _inputSlice, ref objectStoreContext, ref outputFooter);

            //Process output
            string error = default;
            if (status == GarnetStatus.OK)
            {
                ArgSlice[] result = ProcessRespArrayOutput(outputFooter, out error);
                if (error == default)
                {
                    // get the new score
                    _ = Utf8Parser.TryParse(result[0].ReadOnlySpan, out newScore, out _, default);
                }
            }
        }

        return GarnetStatus.OK;
    }

    /// <summary>
    ///
    /// </summary>
    public unsafe GarnetStatus SortedSetLength<TObjectContext>(ArgSlice key, out int zcardCount, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        zcardCount = 0;

        if (key.Length == 0)
            return GarnetStatus.OK;

        ArgSlice input = ScratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);
        // Prepare header in input buffer
        var rmwInput = (ObjectInputHeader*)input.ptr;
        rmwInput->header.type = GarnetObjectType.SortedSet;
        rmwInput->header.flags = 0;
        rmwInput->header.SortedSetOp = SortedSetOperation.ZCARD;
        rmwInput->count = 1;
        rmwInput->done = 0;

        ReadObjectStoreOperation(key.ToArray(), input, out ObjectOutputHeader output, ref objectStoreContext);

        zcardCount = output.opsDone;
        return GarnetStatus.OK;
    }

    /// <summary>
    /// Returns the specified range of elements in the sorted set stored at key, using byscore, bylex and rev modifiers.
    /// Min and max are range boundaries, where 0 is the first element, 1 is the next element and so on.
    /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
    /// </summary>
    public unsafe GarnetStatus SortedSetRange<TObjectContext>(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, ref TObjectContext objectContext, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        elements = default;
        error = default;

        //min and max are required
        if (min.Length == 0 || max.Length == 0)
        {
            //error in arguments
            error = "Missins required min and max parameters";
            return GarnetStatus.NOTFOUND;
        }

        ReadOnlySpan<byte> operation = default;
        SortedSetOperation sortedOperation = SortedSetOperation.ZRANGE;
        switch (sortedSetOrderOperation)
        {
            case SortedSetOrderOperation.ByScore:
                sortedOperation = SortedSetOperation.ZRANGEBYSCORE;
                operation = "BYSCORE"u8;
                break;
            case SortedSetOrderOperation.ByLex:
                sortedOperation = SortedSetOperation.ZRANGE;
                operation = "BYLEX"u8;
                break;
            case SortedSetOrderOperation.ByRank:
                if (reverse)
                    sortedOperation = SortedSetOperation.ZREVRANGE;
                operation = default;
                break;
        }

        // Prepare header in input buffer
        var inputPtr = (ObjectInputHeader*)ScratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
        inputPtr->header.type = GarnetObjectType.SortedSet;
        inputPtr->header.flags = 0;
        inputPtr->header.SortedSetOp = sortedOperation;
        inputPtr->count = 2 + (operation != default ? 1 : 0) + (sortedOperation != SortedSetOperation.ZREVRANGE && reverse ? 1 : 0) + (limit != default ? 3 : 0);
        inputPtr->done = 0;

        int inputLength = sizeof(ObjectInputHeader);

        // min and max parameters
        ArgSlice tmp = ScratchBufferManager.FormatScratchAsResp(0, min, max);
        inputLength += tmp.Length;

        //operation order
        if (operation != default)
        {
            fixed (byte* ptrOp = operation)
            {
                tmp = ScratchBufferManager.FormatScratchAsResp(0, new ArgSlice(ptrOp, operation.Length));
            }
            inputLength += tmp.Length;
        }

        //reverse
        if (sortedOperation != SortedSetOperation.ZREVRANGE && reverse)
        {
            ReadOnlySpan<byte> reverseBytes = "REV"u8;
            fixed (byte* ptrOp = reverseBytes)
            {
                tmp = ScratchBufferManager.FormatScratchAsResp(0, new ArgSlice(ptrOp, reverseBytes.Length));
            }
            inputLength += tmp.Length;
        }

        //limit parameter
        if (limit != default && (sortedSetOrderOperation == SortedSetOrderOperation.ByScore || sortedSetOrderOperation == SortedSetOrderOperation.ByLex))
        {
            ReadOnlySpan<byte> limitBytes = "LIMIT"u8;
            fixed (byte* ptrOp = limitBytes)
            {
                tmp = ScratchBufferManager.FormatScratchAsResp(0, new ArgSlice(ptrOp, limitBytes.Length));
            }
            inputLength += tmp.Length;

            //offset
            byte[] limitOffset = Encoding.ASCII.GetBytes(limit.Item1);
            fixed (byte* ptrOp = limitOffset)
            {
                tmp = ScratchBufferManager.FormatScratchAsResp(0, new ArgSlice(ptrOp, limitOffset.Length));
                inputLength += tmp.Length;
            }

            //count
            int limitCountLength = NumUtils.NumDigitsInLong(limit.Item2);
            byte[] limitCountBytes = new byte[limitCountLength];
            fixed (byte* ptrCount = limitCountBytes)
            {
                byte* ptr = ptrCount;
                NumUtils.IntToBytes(limit.Item2, limitCountLength, ref ptr);
                tmp = ScratchBufferManager.FormatScratchAsResp(0, new ArgSlice(ptrCount, limitCountLength));
                inputLength += tmp.Length;
            }
        }

        ArgSlice input = ScratchBufferManager.GetSliceFromTail(inputLength);
        var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
        GarnetStatus status = ReadObjectStoreOperationWithOutput(key.ToArray(), input, ref objectContext, ref outputFooter);

        if (status == GarnetStatus.OK)
            elements = ProcessRespArrayOutput(outputFooter, out error);

        return status;
    }


    /// <summary>
    /// Computes the difference between the first and all successive sorted sets and returns resulting pairs.
    /// </summary>
    public unsafe GarnetStatus SortedSetDifference(ArgSlice[] keys, out Dictionary<byte[], double> pairs)
    {
        pairs = default;

        if (keys.Length == 0)
            return GarnetStatus.OK;

        bool createTransaction = false;

        if (txnManager.State != TxnState.Running)
        {
            Debug.Assert(txnManager.State == TxnState.None);
            createTransaction = true;
            foreach (ArgSlice item in keys)
                txnManager.SaveKeyEntryToLock(item, true, LockType.Shared);
            txnManager.Run(true);
        }

        LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreLockableContext = txnManager.ObjectStoreLockableContext;

        try
        {
            GarnetStatus statusOp = GET(keys[0].ToArray(), out GarnetObjectStoreOutput firstSortedSet, ref objectStoreLockableContext);
            if (statusOp == GarnetStatus.OK)
            {
                // read the rest of the keys
                for (int item = 1; item < keys.Length; item++)
                {
                    statusOp = GET(keys[item].ToArray(), out GarnetObjectStoreOutput nextSortedSet, ref objectStoreLockableContext);
                    if (statusOp != GarnetStatus.OK)
                    {
                        continue;
                    }
                    if (pairs == default)
                        pairs = SortedSetObject.CopyDiff(((SortedSetObject)firstSortedSet.garnetObject)?.Dictionary, ((SortedSetObject)nextSortedSet.garnetObject)?.Dictionary);
                    else
                        SortedSetObject.InPlaceDiff(pairs, ((SortedSetObject)nextSortedSet.garnetObject)?.Dictionary);
                }
            }
        }
        finally
        {
            if (createTransaction)
                txnManager.Commit(true);
        }

        return GarnetStatus.OK;
    }

    /// <summary>
    /// Iterates members of SortedSet key and their associated scores using a cursor,
    /// a match pattern and count parameters
    /// </summary>
    /// <param name="key">The key of the sorted set</param>
    /// <param name="cursor">The value of the cursor</param>
    /// <param name="match">The pattern to match the members</param>
    /// <param name="count">Limit number for the response</param>
    /// <param name="items">The list of items for the response</param>
    public unsafe GarnetStatus SortedSetScan<TObjectContext>(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items, ref TObjectContext objectStoreContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        items = default;

        if (key.Length == 0)
            return GarnetStatus.OK;

        if (string.IsNullOrEmpty(match))
            match = "*";

        // Prepare header in input buffer
        // Header + ObjectScanCountLimit
        int inputSize = ObjectInputHeader.Size + sizeof(int);
        byte* rmwInput = ScratchBufferManager.CreateArgSlice(inputSize).ptr;
        ((ObjectInputHeader*)rmwInput)->header.type = GarnetObjectType.SortedSet;
        ((ObjectInputHeader*)rmwInput)->header.flags = 0;
        ((ObjectInputHeader*)rmwInput)->header.SortedSetOp = SortedSetOperation.ZSCAN;

        // Number of tokens in the input after the header (match, value, count, value)
        ((ObjectInputHeader*)rmwInput)->count = 4;
        ((ObjectInputHeader*)rmwInput)->done = (int)cursor;
        rmwInput += ObjectInputHeader.Size;

        // Object Input Limit
        *(int*)rmwInput = ObjectScanCountLimit;
        int inputLength = sizeof(ObjectInputHeader) + sizeof(int);

        ArgSlice tmp;
        // Write match
        byte[] matchPatternValue = Encoding.ASCII.GetBytes(match.Trim());
        fixed (byte* matchKeywordPtr = CmdStrings.MATCH, matchPatterPtr = matchPatternValue)
        {
            tmp = ScratchBufferManager.FormatScratchAsResp(0, new ArgSlice(matchKeywordPtr, CmdStrings.MATCH.Length),
                        new ArgSlice(matchPatterPtr, matchPatternValue.Length));
        }
        inputLength += tmp.Length;

        // Write count
        int lengthCountNumber = NumUtils.NumDigits(count);
        byte[] countBytes = new byte[lengthCountNumber];

        fixed (byte* countPtr = CmdStrings.COUNT, countValuePtr = countBytes)
        {
            byte* countValuePtr2 = countValuePtr;
            NumUtils.IntToBytes(count, lengthCountNumber, ref countValuePtr2);

            tmp = ScratchBufferManager.FormatScratchAsResp(0, new ArgSlice(countPtr, CmdStrings.COUNT.Length),
                      new ArgSlice(countValuePtr, countBytes.Length));
        }
        inputLength += tmp.Length;

        ArgSlice input = ScratchBufferManager.GetSliceFromTail(inputLength);

        var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
        GarnetStatus status = ReadObjectStoreOperationWithOutput(key.ToArray(), input, ref objectStoreContext, ref outputFooter);

        items = default;
        if (status == GarnetStatus.OK)
            items = ProcessRespArrayOutput(outputFooter, out _, isScanOutput: true);

        return status;

    }

    /// <summary>
    /// Adds all the specified members with the specified scores to the sorted set stored at key.
    /// Current members get the score updated and reordered.
    /// </summary>
    public GarnetStatus SortedSetAdd<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
    where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

    /// <summary>
    /// Removes the specified members from the sorted set stored at key.
    /// Non existing members are ignored.
    /// </summary>
    public GarnetStatus SortedSetRemove<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

    /// <summary>
    /// Returns the number of members of the sorted set.
    /// </summary>
    public GarnetStatus SortedSetLength<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => ReadObjectStoreOperation(key, input, out output, ref objectStoreContext);

    /// <summary>
    /// Returns the specified range of elements in the sorted set stored at key.
    /// Both start and stop are zero-based indexes, where 0 is the first element, 1 is the next element and so on.
    /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
    /// </summary>
    public GarnetStatus SortedSetRange<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

    /// <summary>
    /// Returns the score of member in the sorted set at key.
    /// If member does not exist in the sorted set, or key does not exist, nil is returned.
    /// </summary>
    public GarnetStatus SortedSetScore<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

    /// <summary>
    /// Returns the scores of members in the sorted set at key.
    /// For every member that does not exist in the sorted set, or if the key does not exist, nil is returned.
    /// </summary>
    public GarnetStatus SortedSetScores<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

    /// <summary>
    /// Removes and returns the first element from the sorted set stored at key,
    /// with the scores ordered from low to high (min) or high to low (max).
    /// </summary>
    public GarnetStatus SortedSetPop<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

    /// <summary>
    /// Returns the number of elements in the sorted set at key with a score between min and max.
    /// </summary>
    public GarnetStatus SortedSetCount<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => ReadObjectStoreOperation(key, input, out output, ref objectContext);

    /// <summary>
    /// Removes all elements in the sorted set between the
    /// lexicographical range specified by min and max.
    /// </summary>
    public GarnetStatus SortedSetRemoveRangeByLex<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperation(key, input, out output, ref objectContext);

    /// <summary>
    /// Returns the number of elements in the sorted set with a value between min and max.
    /// When all the elements in a sorted set have the same score,
    /// this command forces lexicographical ordering.
    /// </summary>
    public GarnetStatus SortedSetLengthByValue<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => ReadObjectStoreOperation(key, input, out output, ref objectStoreContext);

    /// <summary>
    /// Increments the score of member in the sorted set stored at key by increment.
    /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
    /// </summary>
    public GarnetStatus SortedSetIncrement<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

    /// <summary>
    /// ZREMRANGEBYRANK: Removes all elements in the sorted set stored at key with rank between start and stop.
    /// Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
    /// ZREMRANGEBYSCORE: Removes all elements in the sorted set stored at key with a score between min and max (inclusive by default).
    /// </summary>
    public GarnetStatus SortedSetRemoveRange<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperation(key, input, out output, ref objectContext);

    /// <summary>
    /// Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
    /// </summary>
    public GarnetStatus SortedSetRank<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => ReadObjectStoreOperation(key, input, out output, ref objectContext);

    /// <summary>
    /// Returns a random member from the sorted set key.
    /// </summary>
    public GarnetStatus SortedSetRandomMember<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => ReadObjectStoreOperationWithOutput(key, input, ref objectContext, ref outputFooter);

    /// <summary>
    /// Iterates members of SortedSet key and their associated scores using a cursor,
    /// a match pattern and count parameters.
    /// </summary>
    public GarnetStatus SortedSetScan<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
     where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
       => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);
}