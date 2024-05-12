// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using Tsavorite;

namespace Garnet.Server;

/// <summary>
/// State for Functions - one instance per session is created
/// </summary>
internal sealed class FunctionsState
{
    public TsavoriteLog AppendOnlyFile { get; }
    public CustomCommand[] CustomCommands { get; }
    public CustomObjectCommandWrapper[] CustomObjectCommands { get; }
    public WatchVersionMap WatchVersionMap { get; }
    public MemoryPool<byte> MemoryPool { get; }
    public CacheSizeTracker ObjectStoreSizeTracker { get; }
    public GarnetObjectSerializer GarnetObjectSerializer { get; }
    public bool StoredProcMode { get; set;  }

    public FunctionsState(
        TsavoriteLog appendOnlyFile, 
        WatchVersionMap watchVersionMap, 
        CustomCommand[] customCommands, 
        CustomObjectCommandWrapper[] customObjectCommands,
        MemoryPool<byte> memoryPool, 
        CacheSizeTracker objectStoreSizeTracker, 
        GarnetObjectSerializer garnetObjectSerializer)
    {
        AppendOnlyFile = appendOnlyFile;
        WatchVersionMap = watchVersionMap;
        CustomCommands = customCommands;
        CustomObjectCommands = customObjectCommands;
        MemoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        ObjectStoreSizeTracker = objectStoreSizeTracker;
        GarnetObjectSerializer = garnetObjectSerializer;
    }
}