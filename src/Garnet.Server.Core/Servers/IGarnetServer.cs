// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using Garnet.Networking;

namespace Garnet.Server;

/// <summary>
/// 
/// </summary>
public interface IGarnetServer : IDisposable
{
    /// <summary>
    /// Register session provider for specified wire format with the server
    /// </summary>
    public void Register(WireFormat wireFormat, ISessionProvider backendProvider);

    /// <summary>
    /// Unregister provider associated with specified wire format
    /// </summary>
    public void Unregister(WireFormat wireFormat, out ISessionProvider provider);

    /// <summary>
    /// 
    /// </summary>
    public ConcurrentDictionary<WireFormat, ISessionProvider> GetSessionProviders();

    /// <summary>
    /// 
    /// </summary>
    public bool AddSession(WireFormat protocol, ref ISessionProvider provider, INetworkSender networkSender, out IMessageConsumer session);

    /// <summary>
    /// Start server
    /// </summary>
    public void Start();
}