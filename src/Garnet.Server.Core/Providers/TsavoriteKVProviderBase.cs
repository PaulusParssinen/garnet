// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;
using Garnet.networking;
using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// Abstract session provider for TsavoriteKV store based on
/// [K, V, I, O, F, P]
/// </summary>
public abstract class TsavoriteKVProviderBase<Key, Value, Input, Output, Functions, ParameterSerializer> : ISessionProvider
    where Functions : IFunctions<Key, Value, Input, Output, long>
    where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
{
    /// <summary>
    /// Store
    /// </summary>
    protected readonly TsavoriteKV<Key, Value> store;

    /// <summary>
    /// Serializer
    /// </summary>
    protected readonly ParameterSerializer serializer;

    /// <summary>
    /// Broker
    /// </summary>
    protected readonly SubscribeBroker<Key, Value, IKeySerializer<Key>> broker;

    /// <summary>
    /// Size settings
    /// </summary>
    protected readonly MaxSizeSettings maxSizeSettings;

    /// <summary>
    /// Create TsavoriteKV backend
    /// </summary>
    public TsavoriteKVProviderBase(TsavoriteKV<Key, Value> store, ParameterSerializer serializer, SubscribeBroker<Key, Value, IKeySerializer<Key>> broker = null, bool recoverStore = false, MaxSizeSettings maxSizeSettings = default)
    {
        this.store = store;
        if (recoverStore)
        {
            try
            {
                store.Recover();
            }
            catch
            { }
        }
        this.broker = broker;
        this.serializer = serializer;
        this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
    }

    /// <summary>
    /// Get MaxSizeSettings
    /// </summary>
    public MaxSizeSettings GetMaxSizeSettings => this.maxSizeSettings;

    /// <summary>
    /// GetFunctions() for custom functions provided by the client
    /// </summary>
    public abstract Functions GetFunctions();

    /// <inheritdoc />
    public abstract IMessageConsumer GetSession(WireFormat wireFormat, INetworkSender networkSender);
}