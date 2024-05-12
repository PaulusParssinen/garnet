// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Networking;

/// <summary>
/// Interface for consumers of messages (from networks), such as sessions
/// </summary>
public interface IMessageConsumer : IDisposable
{
    /// <summary>
    /// Consume the message incoming on the wire
    /// </summary>
    unsafe int TryConsumeMessages(byte* reqBuffer, int bytesRead);
}