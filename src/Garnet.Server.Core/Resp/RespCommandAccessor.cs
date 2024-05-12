﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Server;

/// <summary>
/// RESP command accessor
/// </summary>
public static class RespCommandAccessor
{
    /// <summary>
    /// MIGRATE
    /// </summary>
    public static byte MIGRATE => (byte)RespCommand.MIGRATE;
}