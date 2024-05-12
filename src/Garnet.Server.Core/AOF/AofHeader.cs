// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.Server;

[StructLayout(LayoutKind.Explicit, Size = 14)]
internal struct AofHeader
{
    [FieldOffset(0)]
    public AofEntryType OpType;
    [FieldOffset(1)]
    public byte Type;
    [FieldOffset(2)]
    public long Version;
    [FieldOffset(10)]
    public int SessionId;
}