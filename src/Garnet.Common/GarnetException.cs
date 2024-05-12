// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Common;

/// <summary>
/// Garnet exception base type
/// </summary>
public class GarnetException : Exception
{
    /// <summary>
    /// Throw Garnet exception
    /// </summary>
    public GarnetException()
    {
    }

    /// <summary>
    /// Throw Garnet exception with message
    /// </summary>
    public GarnetException(string message) : base(message)
    {
    }

    /// <summary>
    /// Throw Garnet exception with message and inner exception
    /// </summary>
    public GarnetException(string message, Exception innerException) : base(message, innerException)
    {
    }
}