// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// Object store functions
/// </summary>
public readonly unsafe partial struct ObjectStoreFunctions : IFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
{
    private readonly FunctionsState functionsState;

    /// <summary>
    /// Constructor
    /// </summary>
    internal ObjectStoreFunctions(FunctionsState functionsState)
    {
        this.functionsState = functionsState;
    }
}