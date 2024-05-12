// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.networking;

/// <summary>
/// Hook for server
/// </summary>
public interface IServerHook
{
    /// <summary>
    /// Try creating a message consumer
    /// </summary>
    bool TryCreateMessageConsumer(Span<byte> bytesReceived, INetworkSender networkSender, out IMessageConsumer session);

    /// <summary>
    /// Dispose message consumer
    /// </summary>
    void DisposeMessageConsumer(INetworkHandler session);

    /// <summary>
    /// Check whether server is disposed
    /// </summary>
    bool Disposed { get; }
}