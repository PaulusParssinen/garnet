﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;

namespace Tsavorite.Tests;

/// <summary>
/// Log formatter primitives
/// </summary>
public static class LogFormatter
{
    private const string TIME_FORMAT = "HH:mm:ss.ffff";
    private const string DATE_FORMAT = "yyyy-MM-dd " + TIME_FORMAT;

    /// <summary>
    /// Format date
    /// </summary>
    public static string FormatDate(DateTime dateTime) => dateTime.ToString(DATE_FORMAT, CultureInfo.InvariantCulture);

    /// <summary>
    /// Format time
    /// </summary>
    public static string FormatTime(DateTime dateTime) => dateTime.ToString(TIME_FORMAT, CultureInfo.InvariantCulture);
}