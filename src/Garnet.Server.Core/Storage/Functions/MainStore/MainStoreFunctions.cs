// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// Callback functions for main store
/// </summary>
public readonly unsafe partial struct MainStoreFunctions : IFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
{
    readonly FunctionsState functionsState;

    /// <summary>
    /// Constructor
    /// </summary>
    internal MainStoreFunctions(FunctionsState functionsState)
    {
        this.functionsState = functionsState;
    }
}