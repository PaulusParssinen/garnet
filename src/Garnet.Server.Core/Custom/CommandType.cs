﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Server
{
    /// <summary>
    /// Type of custom command
    /// </summary>
    public enum CommandType : byte
    {
        /// <summary>
        /// Read
        /// </summary>
        Read,
        /// <summary>
        /// Read-modify-write
        /// </summary>
        ReadModifyWrite
    }
}