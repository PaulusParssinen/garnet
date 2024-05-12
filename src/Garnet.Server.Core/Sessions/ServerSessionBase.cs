// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.networking;

namespace Garnet.Server;

/// <summary>
/// Abstract base class for server session provider
/// </summary>
public abstract class ServerSessionBase : IMessageConsumer
{
    /// <summary>
    /// Bytes read
    /// </summary>
    protected int bytesRead;

    /// <summary>
    /// NetworkSender instance
    /// </summary>
    protected readonly INetworkSender networkSender;

    /// <summary>
    ///  Create instance of session backed by given networkSender
    /// </summary>
    public ServerSessionBase(INetworkSender networkSender)
    {
        this.networkSender = networkSender;
        bytesRead = 0;
    }

    /// <inheritdoc />
    public abstract unsafe int TryConsumeMessages(byte* req_buf, int bytesRead);

    /// <summary>
    /// Publish an update to a key to all the subscribers of the key
    /// </summary>
    public abstract unsafe void Publish(ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength, ref byte* inputPtr, int sid);

    /// <summary>
    /// Publish an update to a key to all the (prefix) subscribers of the key
    /// </summary>
    public abstract unsafe void PrefixPublish(byte* prefixPtr, int prefixLength, ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength, ref byte* inputPtr, int sid);

    /// <summary>
    /// Dispose
    /// </summary>
    public virtual void Dispose() => networkSender?.Dispose();
}