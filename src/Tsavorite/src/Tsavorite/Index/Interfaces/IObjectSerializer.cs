// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;

namespace Tsavorite;

/// <summary>
/// Object serializer interface
/// </summary>
public interface IObjectSerializer<T>
{
    /// <summary>
    /// Begin serialization to given stream
    /// </summary>
    void BeginSerialize(Stream stream);

    /// <summary>
    /// Serialize object
    /// </summary>
    void Serialize(ref T obj);

    /// <summary>
    /// End serialization to given stream
    /// </summary>
    void EndSerialize();

    /// <summary>
    /// Begin deserialization from given stream
    /// </summary>
    void BeginDeserialize(Stream stream);

    /// <summary>
    /// Deserialize object
    /// </summary>
    void Deserialize(out T obj);

    /// <summary>
    /// End deserialization from given stream
    /// </summary>
    void EndDeserialize();
}

/// <summary>
/// Serializer base class for binary reader and writer
/// </summary>
public abstract class BinaryObjectSerializer<T> : IObjectSerializer<T>
{
    /// <summary>
    /// Binary reader
    /// </summary>
    protected BinaryReader reader;

    /// <summary>
    /// Binary writer
    /// </summary>
    protected BinaryWriter writer;

    /// <summary>
    /// Begin deserialization
    /// </summary>
    public void BeginDeserialize(Stream stream)
    {
        reader = new BinaryReader(stream, new UTF8Encoding(), true);
    }

    /// <summary>
    /// Deserialize
    /// </summary>
    public abstract void Deserialize(out T obj);

    /// <summary>
    /// End deserialize
    /// </summary>
    public void EndDeserialize()
    {
        reader.Dispose();
    }

    /// <summary>
    /// Begin serialize
    /// </summary>
    public void BeginSerialize(Stream stream)
    {
        writer = new BinaryWriter(stream, new UTF8Encoding(), true);
    }

    /// <summary>
    /// Serialize
    /// </summary>
    public abstract void Serialize(ref T obj);

    /// <summary>
    /// End serialize
    /// </summary>
    public void EndSerialize()
    {
        writer.Dispose();
    }
}