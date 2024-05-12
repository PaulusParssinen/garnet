// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;
using Garnet.Server;
using Tsavorite;

namespace Garnet;

/// <summary>
/// Functions to implement custom tranasction Write With Expiry - Write with Expiry
/// 
/// Format: WriteWithExpiry 3 key value expiry
/// 
/// Description: Update key to given value with expiry
/// </summary>
sealed class WriteWithExpiryTxn : CustomTransactionProcedure
{
    public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
    {
        int offset = 0;
        AddKey(GetNextArg(input, ref offset), LockType.Exclusive, false);
        return true;
    }

    public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
    {
        int offset = 0;
        ArgSlice key = GetNextArg(input, ref offset);
        ArgSlice value = GetNextArg(input, ref offset);
        ArgSlice expiryMs = GetNextArg(input, ref offset);

        api.SETEX(key, value, expiryMs);
        WriteSimpleString(ref output, "SUCCESS");
    }
}