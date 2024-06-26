// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;
using Garnet.Networking;
using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// Session provider for Garnet, based on
/// [K, V, I, O, C] = [SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long]
/// </summary>
public sealed class GarnetProvider : TsavoriteKVProviderBase<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteServerSerializer>
{
    private readonly StoreWrapper storeWrapper;

    /// <summary>
    /// StoreWrapper
    /// </summary>
    internal StoreWrapper StoreWrapper => storeWrapper;

    /// <summary>
    /// Create SpanByte TsavoriteKV backend for Garnet
    /// </summary>
    public GarnetProvider(StoreWrapper storeWrapper,
        SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> broker = null,
        MaxSizeSettings maxSizeSettings = default)
        : base(storeWrapper.store, new(), broker, false, maxSizeSettings)
    {
        this.storeWrapper = storeWrapper;
    }

    /// <summary>
    /// Start
    /// </summary>
    public void Start()
        => storeWrapper.Start();

    /// <summary>
    /// Recover
    /// </summary>
    public void Recover()
        => storeWrapper.Recover();

    /// <summary>
    /// Dispose
    /// </summary>
    public void Dispose()
    {
        storeWrapper.Dispose();
    }

    /// <inheritdoc />
    public override SpanByteFunctionsForServer<long> GetFunctions() => new();

    /// <inheritdoc />
    public override IMessageConsumer GetSession(WireFormat wireFormat, INetworkSender networkSender)
        => (wireFormat == WireFormat.ASCII)
            ? new RespServerSession(networkSender, storeWrapper, broker)
            : throw new GarnetException($"Unsupported wireFormat {wireFormat}");
}