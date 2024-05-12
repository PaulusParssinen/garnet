// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Tsavorite;

namespace Garnet.Server;

sealed partial class StorageSession
{
    /// <summary>
    /// Handles the complete pending status for Session Store
    /// </summary>
    static void CompletePendingForSession<TContext>(ref Status status, ref SpanByteAndMemory output, ref TContext context)
        where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
    {
        context.CompletePendingWithOutputs(out CompletedOutputIterator<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long> completedOutputs, wait: true);
        bool more = completedOutputs.Next();
        Debug.Assert(more);
        status = completedOutputs.Current.Status;
        output = completedOutputs.Current.Output;
        more = completedOutputs.Next();
        Debug.Assert(!more);
        completedOutputs.Dispose();
    }
}