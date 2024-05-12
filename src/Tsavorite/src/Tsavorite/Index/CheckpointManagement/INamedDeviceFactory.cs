// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.Device;

namespace Tsavorite;

/// <summary>
/// Factory for getting IDevice instances for checkpointing
/// </summary>
public interface INamedDeviceFactory
{
    /// <summary>
    /// Initialize base name or container
    /// </summary>
    /// <param name="baseName">Base name or container</param>
    void Initialize(string baseName);

    /// <summary>
    /// Get IDevice instance for given file info
    /// </summary>
    /// <param name="fileInfo">File info</param>
    IDevice Get(FileDescriptor fileInfo);

    /// <summary>
    /// Delete IDevice for given file info
    /// </summary>
    /// <param name="fileInfo">File info</param>
    void Delete(FileDescriptor fileInfo);

    /// <summary>
    /// List path contents, in order of preference
    /// </summary>
    IEnumerable<FileDescriptor> ListContents(string path);
}