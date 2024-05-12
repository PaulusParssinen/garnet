// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace GarnetClientSample;

/// <summary>
/// Use Garnet with GarnetClient and StackExchange.Redis clients
/// </summary>
internal class Program
{
    private static readonly string address = "127.0.0.1";
    private static readonly int port = 3278;
    private static readonly bool useTLS = false;

    private static async Task Main()
    {
        await new GarnetClientSamples(address, port, useTLS).RunAll();

        // await new SERedisSamples(address, port).RunAll();
    }
}