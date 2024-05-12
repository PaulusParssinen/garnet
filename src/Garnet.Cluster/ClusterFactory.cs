﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Server;
using Microsoft.Extensions.Logging;
using Tsavorite;

namespace Garnet.Cluster
{
    /// <summary>
    /// Cluster factory
    /// </summary>
    public class ClusterFactory : IClusterFactory
    {
        /// <inheritdoc />
        public DeviceLogCommitCheckpointManager CreateCheckpointManager(INamedDeviceFactory deviceFactory, ICheckpointNamingScheme checkpointNamingScheme, bool isMainStore, ILogger logger = default)
            => new ReplicationLogCheckpointManager(deviceFactory, checkpointNamingScheme, isMainStore, logger: logger);

        /// <inheritdoc />
        public IClusterProvider CreateClusterProvider(StoreWrapper store)
            => new ClusterProvider(store);
    }
}