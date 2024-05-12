// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Sockets;
using Garnet.Common;
using Microsoft.Extensions.Logging;

namespace Garnet.Server;

internal sealed class ServerTcpNetworkHandler : TcpNetworkHandler<GarnetServerTcp>
{
    public ServerTcpNetworkHandler(GarnetServerTcp serverHook, Socket socket, LimitedFixedBufferPool networkPool, bool useTLS, int networkSendThrottleMax, ILogger logger = null)
        : base(serverHook, socket, networkPool, useTLS, null, networkSendThrottleMax, logger)
    {
    }
}