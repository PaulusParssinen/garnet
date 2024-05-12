// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite;

namespace Garnet.Server;

sealed partial class StorageSession : IDisposable
{
    /// <summary>
    /// Adds new elements at the head(right) or tail(left)
    /// in the list stored at key.
    /// For the case of ListPushX, the operation is only done if the key already exists
    /// and holds a list.
    /// </summary>
    /// <param name="key">The name of the key</param>
    /// <param name="elements">The elements to be added at the left or the righ of the list</param>
    /// <param name="lop">The Right or Left modifier of the operation to perform</param>
    /// <param name="itemsDoneCount">The length of the list after the push operations.</param>
    public unsafe GarnetStatus ListPush<TObjectContext>(ArgSlice key, ArgSlice[] elements, ListOperation lop, out int itemsDoneCount, ref TObjectContext objectStoreContext)
      where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        itemsDoneCount = 0;

        if (key.Length == 0 || elements.Length == 0)
            return GarnetStatus.OK;

        // Prepare header in buffer
        var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
        rmwInput->header.type = GarnetObjectType.List;
        rmwInput->header.flags = 0;
        rmwInput->header.ListOp = lop;
        rmwInput->count = elements.Length;
        rmwInput->done = 0;

        //Iterate through all inputs and add them to the scratch buffer in RESP format
        int inputLength = sizeof(ObjectInputHeader);
        foreach (ArgSlice item in elements)
        {
            ArgSlice tmp = scratchBufferManager.FormatScratchAsResp(0, item);
            inputLength += tmp.Length;
        }

        ArgSlice input = scratchBufferManager.GetSliceFromTail(inputLength);
        RMWObjectStoreOperation(key.ToArray(), input, out ObjectOutputHeader output, ref objectStoreContext);

        itemsDoneCount = output.countDone;
        return GarnetStatus.OK;
    }

    /// <summary>
    /// Adds new elements at the head(right) or tail(left)
    /// in the list stored at key.
    /// For the case of ListPushX, the operation is only done if the key already exists
    /// and holds a list.
    /// </summary>
    public unsafe GarnetStatus ListPush<TObjectContext>(ArgSlice key, ArgSlice element, ListOperation lop, out int itemsDoneCount, ref TObjectContext objectStoreContext)
       where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        itemsDoneCount = 0;

        ArgSlice input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, element);

        // Prepare header in input buffer
        var rmwInput = (ObjectInputHeader*)input.ptr;
        rmwInput->header.type = GarnetObjectType.List;
        rmwInput->header.flags = 0;
        rmwInput->header.ListOp = lop;
        rmwInput->count = 1;
        rmwInput->done = 0;

        GarnetStatus status = RMWObjectStoreOperation(key.ToArray(), element, out ObjectOutputHeader output, ref objectStoreContext);
        itemsDoneCount = output.countDone;

        return status;
    }

    /// <summary>
    /// Removes one element from the head(left) or tail(right) 
    /// of the list stored at key.
    /// </summary>
    /// <returns>The popped element</returns>
    public GarnetStatus ListPop<TObjectContext>(ArgSlice key, ListOperation lop, ref TObjectContext objectStoreContext, out ArgSlice element)
       where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        GarnetStatus status = ListPop(key, 1, lop, ref objectStoreContext, out ArgSlice[] elements);
        element = elements.FirstOrDefault();
        return status;
    }

    /// <summary>
    /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
    /// If the list contains less than count elements, removes and returns the number of elements in the list.
    /// </summary>
    /// <returns>The count elements popped from the list</returns>
    public unsafe GarnetStatus ListPop<TObjectContext>(ArgSlice key, int count, ListOperation lop, ref TObjectContext objectStoreContext, out ArgSlice[] elements)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        byte[] _key = key.ToArray();
        SpanByte _keyAsSpan = key.SpanByte;

        // Construct input for operation
        ArgSlice input = scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size);

        // Prepare header in input buffer
        var rmwInput = (ObjectInputHeader*)input.ptr;
        rmwInput->header.type = GarnetObjectType.List;
        rmwInput->header.flags = 0;
        rmwInput->header.ListOp = lop;
        rmwInput->count = count;
        rmwInput->done = 0;

        var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

        GarnetStatus status = RMWObjectStoreOperationWithOutput(key.ToArray(), input, ref objectStoreContext, ref outputFooter);

        //process output
        elements = default;
        if (status == GarnetStatus.OK)
            elements = ProcessRespArrayOutput(outputFooter, out string error);

        return GarnetStatus.OK;
    }

    /// <summary>
    /// Gets the current count of elements in the List at Key
    /// </summary>
    public unsafe GarnetStatus ListLength<TObjectContext>(ArgSlice key, ref TObjectContext objectStoreContext, out int count)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        count = 0;

        if (key.Length == 0)
            return GarnetStatus.OK;

        ArgSlice input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);

        // Prepare header in input buffer
        var rmwInput = (ObjectInputHeader*)input.ptr;
        rmwInput->header.type = GarnetObjectType.List;
        rmwInput->header.flags = 0;
        rmwInput->header.ListOp = ListOperation.LLEN;
        rmwInput->count = count;
        rmwInput->done = 0;

        GarnetStatus status = ReadObjectStoreOperation(key.ToArray(), input, out ObjectOutputHeader output, ref objectStoreContext);

        count = output.countDone;
        return status;
    }

    /// <summary>
    /// Removes the first/last element of the list stored at source
    /// and pushes it to the first/last element of the list stored at destination
    /// </summary>
    /// <param name="element">out parameter, The element being popped and pushed</param>
    /// <returns>true when success</returns>
    public bool ListMove(ArgSlice sourceKey, ArgSlice destinationKey, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element)
    {
        element = default;
        LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectLockableContext = txnManager.ObjectStoreLockableContext;

        //If source and destination are the same, the operation is equivalent to removing the last element from the list
        //and pushing it as first element of the list, so it can be considered as a list rotation command.
        bool sameKey = sourceKey.ReadOnlySpan.SequenceEqual(destinationKey.ReadOnlySpan);

        bool createTransaction = false;
        if (txnManager.state != TxnState.Running)
        {
            createTransaction = true;
            txnManager.SaveKeyEntryToLock(sourceKey, true, LockType.Exclusive);
            txnManager.SaveKeyEntryToLock(destinationKey, true, LockType.Exclusive);
            txnManager.Run(true);
        }

        LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreLockableContext = txnManager.ObjectStoreLockableContext;

        try
        {
            // get the source key
            GarnetStatus statusOp = GET(sourceKey.ToArray(), out GarnetObjectStoreOutput sourceList, ref objectLockableContext);

            if (statusOp == GarnetStatus.NOTFOUND || ((ListObject)sourceList.garnetObject).LnkList.Count == 0)
            {
                return true;
            }
            else if (statusOp == GarnetStatus.OK)
            {
                var srcListObject = (ListObject)sourceList.garnetObject;

                // right pop (removelast) from source
                if (sourceDirection == OperationDirection.Right)
                {
                    element = srcListObject.LnkList.Last.Value;
                    srcListObject.LnkList.RemoveLast();
                }
                else
                {
                    // left pop (removefirst) from source
                    element = srcListObject.LnkList.First.Value;
                    srcListObject.LnkList.RemoveFirst();
                }
                srcListObject.UpdateSize(element, false);

                //update sourcelist
                SET(sourceKey.ToArray(), sourceList.garnetObject, ref objectStoreLockableContext);

                IGarnetObject newListValue = null;
                if (!sameKey)
                {
                    // read destination key
                    byte[] _destinationKey = destinationKey.ToArray();
                    statusOp = GET(_destinationKey, out GarnetObjectStoreOutput destinationList, ref objectStoreLockableContext);

                    if (statusOp == GarnetStatus.NOTFOUND)
                    {
                        destinationList.garnetObject = new ListObject();
                    }

                    var dstListObject = (ListObject)destinationList.garnetObject;

                    //left push (addfirst) to destination
                    if (destinationDirection == OperationDirection.Left)
                        dstListObject.LnkList.AddFirst(element);
                    else
                        dstListObject.LnkList.AddLast(element);

                    dstListObject.UpdateSize(element);
                    newListValue = new ListObject(dstListObject.LnkList, dstListObject.Expiration, dstListObject.Size);
                }
                else
                {
                    // when the source and the destination key is the same the operation is done only in the sourceList
                    if (sourceDirection == OperationDirection.Right && destinationDirection == OperationDirection.Left)
                        srcListObject.LnkList.AddFirst(element);
                    else if (sourceDirection == OperationDirection.Left && destinationDirection == OperationDirection.Right)
                        srcListObject.LnkList.AddLast(element);
                    newListValue = sourceList.garnetObject;
                    ((ListObject)newListValue).UpdateSize(element);
                }

                // upsert
                SET(destinationKey.ToArray(), newListValue, ref objectStoreLockableContext);
            }
        }
        finally
        {
            if (createTransaction)
                txnManager.Commit(true);
        }

        return true;

    }

    /// <summary>
    /// Trim an existing list so it only contains the specified range of elements.
    /// </summary>
    /// <returns>true when successful</returns>
    public unsafe bool ListTrim<TObjectContext>(ArgSlice key, int start, int stop, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        ArgSlice input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);

        // Prepare header in input buffer
        var rmwInput = (ObjectInputHeader*)input.ptr;
        rmwInput->header.type = GarnetObjectType.List;
        rmwInput->header.flags = 0;
        rmwInput->header.ListOp = ListOperation.LTRIM;
        rmwInput->count = start;
        rmwInput->done = stop;

        GarnetStatus status = RMWObjectStoreOperation(key.ToArray(), input, out ObjectOutputHeader output, ref objectStoreContext);

        return status == GarnetStatus.OK;
    }

    /// <summary>
    /// Adds new elements at the head(right) or tail(left)
    /// </summary>
    public GarnetStatus ListPush<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

    /// <summary>
    /// Trim an existing list so it only contains the specified range of elements.
    /// </summary>
    public GarnetStatus ListTrim<TObjectContext>(byte[] key, ArgSlice input, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperation(key, input, out _, ref objectStoreContext);

    /// <summary>
    /// Gets the specified elements of the list stored at key.
    /// </summary>
    public GarnetStatus ListRange<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

    /// <summary>
    /// Inserts a new element in the list stored at key either before or after a value pivot
    /// </summary>
    public GarnetStatus ListInsert<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

    /// <summary>
    /// Returns the element at index.
    /// </summary>
    public GarnetStatus ListIndex<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

    /// <summary>
    /// Removes the first count occurrences of elements equal to element from the list.
    /// LREM key count element
    /// </summary>
    public GarnetStatus ListRemove<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

    /// <summary>
    /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
    /// If the list contains less than count elements, removes and returns the number of elements in the list.
    /// </summary>
    public unsafe GarnetStatus ListPop<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

    /// <summary>
    /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
    /// If the list contains less than count elements, removes and returns the number of elements in the list.
    /// </summary>
    public unsafe GarnetStatus ListLength<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
         => ReadObjectStoreOperation(key, input, out output, ref objectStoreContext);

    /// <summary>
    /// Sets the list element at index to element.
    /// </summary>
    public unsafe GarnetStatus ListSet<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => RMWObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);
}