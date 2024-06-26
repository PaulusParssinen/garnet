﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite;

namespace Garnet.Server;

internal sealed partial class StorageSession : IDisposable
{
    public GarnetStatus RMW_ObjectStore<TObjectContext>(ref byte[] key, ref SpanByte input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
        where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        Status status = objectStoreContext.RMW(ref key, ref input, ref output);

        if (status.IsPending)
            CompletePendingForObjectStoreSession(ref status, ref output, ref objectStoreContext);

        if (status.Found)
            return GarnetStatus.OK;
        else
            return GarnetStatus.NOTFOUND;
    }

    public GarnetStatus Read_ObjectStore<TObjectContext>(ref byte[] key, ref SpanByte input, ref GarnetObjectStoreOutput output, ref TObjectContext objectStoreContext)
    where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        Status status = objectStoreContext.Read(ref key, ref input, ref output);

        if (status.IsPending)
            CompletePendingForObjectStoreSession(ref status, ref output, ref objectStoreContext);

        if (status.Found)
            return GarnetStatus.OK;
        else
            return GarnetStatus.NOTFOUND;
    }
}