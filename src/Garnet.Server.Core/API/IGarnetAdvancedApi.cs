﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// Advanced API calls, not to be used by normal clients
/// </summary>
public interface IGarnetAdvancedApi
{
    /// <summary>
    /// GET with support for pending multiple ongoing operations, scatter gather IO for outputs
    /// </summary>
    GarnetStatus GET_WithPending(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, long ctx, out bool pending);

    /// <summary>
    /// Complete pending read operations on main store
    /// </summary>
    bool GET_CompletePending((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait = false);

    /// <summary>
    /// RMW operation on main store
    /// </summary>
    GarnetStatus RMW_MainStore(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output);

    /// <summary>
    /// Read operation on main store
    /// </summary>
    GarnetStatus Read_MainStore(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output);

    /// <summary>
    /// RMW operation on object store
    /// </summary>
    GarnetStatus RMW_ObjectStore(ref byte[] key, ref SpanByte input, ref GarnetObjectStoreOutput output);

    /// <summary>
    /// Read operation on object store
    /// </summary>
    GarnetStatus Read_ObjectStore(ref byte[] key, ref SpanByte input, ref GarnetObjectStoreOutput output);
}