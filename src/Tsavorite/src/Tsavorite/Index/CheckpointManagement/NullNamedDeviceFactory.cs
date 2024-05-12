// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.Device;

namespace Tsavorite;

/// <summary>
/// Local storage device factory
/// </summary>
public class NullNamedDeviceFactory : INamedDeviceFactory
{
    private static readonly NullDevice nullDevice = new();

    /// <summary>
    /// Create instance of factory
    /// </summary>
    public NullNamedDeviceFactory() { }

    /// <inheritdoc />
    public void Delete(FileDescriptor fileInfo) { }

    /// <inheritdoc />
    public IDevice Get(FileDescriptor fileInfo) => nullDevice;

    /// <inheritdoc />
    public void Initialize(string baseName) { }

    /// <inheritdoc />
    public IEnumerable<FileDescriptor> ListContents(string path)
    {
        yield break;
    }
}