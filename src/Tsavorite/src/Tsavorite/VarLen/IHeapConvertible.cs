// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite;

/// <summary>
/// Whether type supports converting to heap (e.g., when operation goes pending)
/// </summary>
public interface IHeapConvertible
{
    /// <summary>
    /// Convert to heap
    /// </summary>
    public void ConvertToHeap();
}