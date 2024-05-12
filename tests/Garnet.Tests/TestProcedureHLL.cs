// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.Common;
using Garnet.Server;
using Tsavorite;

namespace Garnet;

/// <summary>
/// Test procedure to use HyperLogLog Commands in Garnet API
/// 
/// Format: HLLPROC hll e1 e2 e3 e4 e5 e6 e7
/// 
/// Description: Exercise PFADD PFCOUNT
/// </summary>

internal sealed class TestProcedureHLL : CustomTransactionProcedure
{
    public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
    {
        int offset = 0;
        ArgSlice hll = GetNextArg(input, ref offset);

        if (hll.Length == 0)
            return false;

        AddKey(hll, LockType.Exclusive, false);
        return true;
    }

    public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
    {
        int offset = 0;
        string[] elements = new string[7];
        bool result = true;

        ArgSlice hll = GetNextArg(input, ref offset);

        if (hll.Length == 0)
            result = false;

        if (result)
        {
            for (int i = 0; i < elements.Length; i++)
            {
                elements[i] = Encoding.ASCII.GetString(GetNextArg(input, ref offset).ToArray());
            }
            api.HyperLogLogAdd(hll, elements, out bool resultPfAdd);
            result = resultPfAdd;
            api.HyperLogLogLength([hll], out long count);
            if (count != 7)
            {
                result = false;
            }
        }
        WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
    }
}