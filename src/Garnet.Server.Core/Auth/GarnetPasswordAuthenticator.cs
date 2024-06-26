﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Server.ACL;

namespace Garnet.Server.Auth;

/// <summary>
/// Authenticator that uses a single fixed password.
/// XXX: Deprecated. Should be replaced by ACL authenticator.
/// </summary>
internal class GarnetPasswordAuthenticator : IGarnetAuthenticator
{
    public bool IsAuthenticated => _authenticated;

    public bool CanAuthenticate => true;

    public bool HasACLSupport => false;

    private readonly byte[] _pwd;
    private bool _authenticated;

    public GarnetPasswordAuthenticator(byte[] pwd)
    {
        _pwd = pwd;
    }

    public bool Authenticate(ReadOnlySpan<byte> password, ReadOnlySpan<byte> username)
    {
        _authenticated = SecretsUtility.ConstantEquals(_pwd, password);
        return _authenticated;
    }
}