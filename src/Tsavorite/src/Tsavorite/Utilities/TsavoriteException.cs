// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite;

/// <summary>
/// Tsavorite exception base type
/// </summary>
public class TsavoriteException : Exception
{
    /// <summary>
    /// Throw Tsavorite exception
    /// </summary>
    public TsavoriteException()
    {
    }

    /// <summary>
    /// Throw Tsavorite exception with message
    /// </summary>
    public TsavoriteException(string message) : base(message)
    {
    }

    /// <summary>
    /// Throw Tsavorite exception with message and inner exception
    /// </summary>
    public TsavoriteException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Tsavorite IO exception type with message and inner exception
/// </summary>
public class TsavoriteIOException : TsavoriteException
{
    /// <summary>
    /// Throw Tsavorite exception
    /// </summary>
    public TsavoriteIOException(string message, Exception innerException) : base(message, innerException)
    {
    }
}