// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Sockets;
using Garnet.Networking;
using Microsoft.Extensions.Logging;

namespace Garnet.Common;

/// <summary>
/// TCP network handler
/// </summary>
public abstract class TcpNetworkHandler<TServerHook> : TcpNetworkHandlerBase<TServerHook, GarnetTcpNetworkSender>
    where TServerHook : IServerHook
{
    /// <summary>
    /// Constructor
    /// </summary>
    public TcpNetworkHandler(TServerHook serverHook, Socket socket, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, int networkSendThrottleMax = 8, ILogger logger = null)
        : base(serverHook, new GarnetTcpNetworkSender(socket, networkPool, networkSendThrottleMax), socket, networkPool, useTLS, messageConsumer, logger)
    {
    }
}