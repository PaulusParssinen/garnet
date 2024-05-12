// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Server;

public sealed class SortedSetComparer : IComparer<(double, byte[])>
{
    /// <summary>
    /// The default instance.
    /// </summary>
    /// <remarks>Used to avoid allocating new comparers.</remarks>
    public static readonly SortedSetComparer Instance = new();

    /// <inheritdoc/>
    public int Compare((double, byte[]) x, (double, byte[]) y)
    {
        int ret = x.Item1.CompareTo(y.Item1);
        if (ret == 0)
            return new ReadOnlySpan<byte>(x.Item2).SequenceCompareTo(y.Item2);
        return ret;
    }
}