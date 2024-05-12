// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite;

namespace Garnet.Server;

internal sealed partial class StorageSession : IDisposable
{
    /// <summary>
    /// Adds the specified geospatial items (longitude, latitude, name) to the specified key.
    /// Data is stored into the key as a sorted set.
    /// </summary>
    public GarnetStatus GeoAdd<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
      where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
      => RMWObjectStoreOperation(key, input, out output, ref objectContext);

    /// <summary>
    /// GEOHASH: Returns valid Geohash strings representing the position of one or more elements in a geospatial data of the sorted set.
    /// GEODIST: Returns the distance between two members in the geospatial index represented by the sorted set.
    /// GEOPOS: Returns the positions (longitude,latitude) of all the specified members in the sorted set.
    /// GEOSEARCH: Returns the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
    /// </summary>
    public GarnetStatus GeoCommands<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
      where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        => ReadObjectStoreOperationWithOutput(key, input, ref objectContext, ref outputFooter);

}