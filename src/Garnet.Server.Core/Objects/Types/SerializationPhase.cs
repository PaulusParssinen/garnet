// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Server;

internal enum SerializationPhase : int
{
    REST,
    SERIALIZING,
    SERIALIZED,
    DONE
}