﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.Server;

[StructLayout(LayoutKind.Explicit, Size = 4)]
internal struct SerializationState
{
    [FieldOffset(0)]
    public SerializationPhase phase;

    [FieldOffset(4)]
    public int word;

    public static SerializationState Make(SerializationPhase serializationPhase)
    {
        SerializationState state = default;
        state.phase = serializationPhase;
        return state;
    }
}