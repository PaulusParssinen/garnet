﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Tests;

public class CredentialManager
{
    public string aclFilePath = null;

    /// <summary>
    /// Server credentials generated by calling TestUtils.GenerateCredentials
    /// </summary>
    public readonly ServerCredential[] defaultCreds = [
            new ServerCredential("admin", "adminplaceholder", IsAdmin: true, IsClearText: false),
        new ServerCredential("default", "defaultplaceholder", IsAdmin: false, IsClearText: true),
    ];

    /// <summary>
    /// Generate credential ACL file.
    /// </summary>
    /// <param name="TestFolder">Root folder where to write file.</param>
    /// <param name="customCreds">Custom array of credential.</param>
    /// <returns>Returns path to generated file.</returns>
    public void GenerateCredentials(string TestFolder, ServerCredential[] customCreds = null)
    {
        customCreds ??= defaultCreds;

        string aclConfig = "";
        foreach (ServerCredential cred in customCreds)
        {
            if (aclConfig.Length > 0) aclConfig += "\r\n";

            string user = cred.user;
            string password = cred.IsClearText ? cred.password : Convert.ToHexString(cred.hash);
            aclConfig += $"user {user} on " +
                $"{(cred.IsClearText ? ">" : "#")}{password} " +
                $"{(cred.IsAdmin ? "+@admin" : "-@admin")}";
        }

        _ = Directory.CreateDirectory(TestFolder);
        aclFilePath = Path.Join(TestFolder, "users.acl");
        File.WriteAllText(aclFilePath, aclConfig);
    }

    /// <summary>
    /// Get credential for specified user.
    /// </summary>
    /// <param name="user">Username to search for.</param>
    /// <returns>Returns credential object for specified user if found or an empty object.</returns>
    public ServerCredential GetUserCredentials(string user)
    {
        foreach (ServerCredential cred in defaultCreds)
            if (cred.user == user)
                return cred;
        return new ServerCredential();
    }
}