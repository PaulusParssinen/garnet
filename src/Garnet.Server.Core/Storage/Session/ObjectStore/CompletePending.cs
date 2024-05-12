// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Tsavorite;

namespace Garnet.Server;

internal sealed partial class StorageSession
{
    /// <summary>
    /// Handles the complete pending for Object Store session
    /// </summary>
    private static void CompletePendingForObjectStoreSession<TContext>(ref Status status, ref GarnetObjectStoreOutput output, ref TContext objectContext)
        where TContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        objectContext.CompletePendingWithOutputs(out CompletedOutputIterator<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long> completedOutputs, wait: true);
        bool more = completedOutputs.Next();
        Debug.Assert(more);
        status = completedOutputs.Current.Status;
        output = completedOutputs.Current.Output;
        Debug.Assert(!completedOutputs.Next());
        completedOutputs.Dispose();
    }
}