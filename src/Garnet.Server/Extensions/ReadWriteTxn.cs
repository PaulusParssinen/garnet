﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;
using Garnet.Server;
using Tsavorite;

namespace Garnet;

/// <summary>
/// Functions to implement custom tranasction READWRITE - read one key, write to two keys
/// 
/// Format: READWRITE 3 readkey writekey1 writekey2
/// 
/// Description: Update key to given value only if the given prefix matches the 
/// existing value's prefix. If it does not match (or there is no existing value), 
/// then do nothing.
/// </summary>
internal sealed class ReadWriteTxn : CustomTransactionProcedure
{
    public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
    {
        int offset = 0;
        api.GET(GetNextArg(input, ref offset), out ArgSlice key1);
        if (key1.ReadOnlySpan.SequenceEqual("wrong_string"u8))
            return false;
        AddKey(GetNextArg(input, ref offset), LockType.Exclusive, false);
        AddKey(GetNextArg(input, ref offset), LockType.Exclusive, false);
        return true;
    }

    public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
    {
        int offset = 0;
        ArgSlice key1 = GetNextArg(input, ref offset);
        ArgSlice key2 = GetNextArg(input, ref offset);
        ArgSlice key3 = GetNextArg(input, ref offset);

        GarnetStatus status = api.GET(key1, out ArgSlice result);
        if (status == GarnetStatus.OK)
        {
            api.SET(key2, result);
            api.SET(key3, result);
        }
        WriteSimpleString(ref output, "SUCCESS");
    }
}