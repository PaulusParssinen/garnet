﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.Serialization;
using System.Xml;

namespace Tsavorite;

/// <summary>
/// Serializer (for class types) based on DataContract
/// </summary>
public sealed class DataContractObjectSerializer<T> : BinaryObjectSerializer<T>
{
    private static readonly DataContractSerializer serializer = new DataContractSerializer(typeof(T));

    /// <summary>
    /// Deserialize
    /// </summary>
    public override void Deserialize(out T obj)
    {
        int count = reader.ReadInt32();
        byte[] byteArray = reader.ReadBytes(count);
        using var ms = new MemoryStream(byteArray);
        using var _reader = XmlDictionaryReader.CreateBinaryReader(ms, XmlDictionaryReaderQuotas.Max);
        obj = (T)serializer.ReadObject(_reader);
    }

    /// <summary>
    /// Serialize
    /// </summary>
    public override void Serialize(ref T obj)
    {
        using var ms = new MemoryStream();
        using (var _writer = XmlDictionaryWriter.CreateBinaryWriter(ms, null, null, false))
            serializer.WriteObject(_writer, obj);
        writer.Write((int)ms.Position);
        writer.Write(ms.ToArray());
    }
}