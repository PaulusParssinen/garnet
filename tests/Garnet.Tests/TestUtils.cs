﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Security;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Garnet.Common;
using Garnet.Server;
using Garnet.Server.Auth;
using Garnet.Server.TLS;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;
using Tsavorite;

namespace Garnet.Tests;

internal static class TestUtils
{
    /// <summary>
    /// Address
    /// </summary>
    public static string Address = "127.0.0.1";

    /// <summary>
    /// Port
    /// </summary>
    public static int Port = 33278;

    /// <summary>
    /// Whether to use a test progress logger
    /// </summary>
    private static readonly bool useTestLogger = false;

    private static int procId = Process.GetCurrentProcess().Id;
    private static string CustomRespCommandInfoJsonPath = "CustomRespCommandsInfo.json";

    private static bool CustomCommandsInfoInitialized;
    private static IReadOnlyDictionary<string, RespCommandsInfo> RespCustomCommandsInfo;

    public const string certFile = "testcert.pfx";
    public const string certPassword = "placeholder";

    /// <summary>
    /// Get command info for custom commands defined in custom commands json file
    /// </summary>
    /// <param name="customCommandsInfo">Mapping between command name and command info</param>
    /// <param name="logger">Logger</param>
    internal static bool TryGetCustomCommandsInfo(out IReadOnlyDictionary<string, RespCommandsInfo> customCommandsInfo, ILogger logger = null)
    {
        customCommandsInfo = default;

        if (!CustomCommandsInfoInitialized && !TryInitializeCustomCommandsInfo(logger)) return false;

        customCommandsInfo = RespCustomCommandsInfo;
        return true;
    }

    private static bool TryInitializeCustomCommandsInfo(ILogger logger)
    {
        if (!TryGetRespCommandsInfo(CustomRespCommandInfoJsonPath, logger, out IReadOnlyDictionary<string, RespCommandsInfo> tmpCustomCommandsInfo))
            return false;

        RespCustomCommandsInfo = tmpCustomCommandsInfo;
        CustomCommandsInfoInitialized = true;
        return true;
    }

    private static bool TryGetRespCommandsInfo(string resourcePath, ILogger logger, out IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo)
    {
        commandsInfo = default;

        IStreamProvider streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource,  Assembly.GetExecutingAssembly());
        IRespCommandsInfoProvider commandsInfoProvider = RespCommandsInfoProviderFactory.GetRespCommandsInfoProvider();

        bool importSucceeded = commandsInfoProvider.TryImportRespCommandsInfo(resourcePath,
            streamProvider, out IReadOnlyDictionary<string, RespCommandsInfo> tmpCommandsInfo, logger);

        if (!importSucceeded) return false;

        commandsInfo = tmpCommandsInfo;
        return true;
    }

    /// <summary>
    /// Create GarnetServer
    /// </summary>
    public static GarnetServer CreateGarnetServer(
        string logCheckpointDir,
        bool disablePubSub = false,
        bool tryRecover = false,
        bool lowMemory = false,
        string MemorySize = default,
        string PageSize = default,
        bool enableAOF = false,
        bool EnableTLS = false,
        bool DisableObjects = false,
        int metricsSamplingFreq = -1,
        bool latencyMonitor = false,
        int commitFrequencyMs = 0,
        bool commitWait = false,
        string defaultPassword = null,
        bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
        string aclFile = null,
        string objectStoreTotalMemorySize = default,
        string objectStoreIndexSize = "16k",
        string objectStoreIndexMaxSize = default,
        string indexSize = "1m",
        string indexMaxSize = default,
        string[] extensionBinPaths = null,
        bool extensionAllowUnsignedAssemblies = true,
        bool getSG = false,
        int indexResizeFrequencySecs = 60,
        ILogger logger = null)
    {
        string _LogDir = logCheckpointDir;
        if (logCheckpointDir != null) _LogDir = new DirectoryInfo(string.IsNullOrEmpty(_LogDir) ? "." : _LogDir).FullName;

        string _CheckpointDir = logCheckpointDir;
        if (logCheckpointDir != null) _CheckpointDir = new DirectoryInfo(string.IsNullOrEmpty(_CheckpointDir) ? "." : _CheckpointDir).FullName;

        IAuthenticationSettings authenticationSettings = null;
        if (useAcl)
        {
            authenticationSettings = new AclAuthenticationSettings(aclFile, defaultPassword);
        }
        else if (defaultPassword != null)
        {
            authenticationSettings = new PasswordAuthenticationSettings(defaultPassword);
        }

        // Increase minimum thread pool size to 16 if needed
        int threadPoolMinThreads = 0;
        ThreadPool.GetMinThreads(out int workerThreads, out int completionPortThreads);
        if (workerThreads < 16 || completionPortThreads < 16) threadPoolMinThreads = 16;

        GarnetServerOptions opts = new(logger)
        {
            EnableStorageTier = logCheckpointDir != null,
            LogDir = _LogDir,
            CheckpointDir = _CheckpointDir,
            Address = Address,
            Port = Port,
            DisablePubSub = disablePubSub,
            Recover = tryRecover,
            IndexSize = indexSize,
            ObjectStoreIndexSize = objectStoreIndexSize,
            EnableAOF = enableAOF,
            CommitFrequencyMs = commitFrequencyMs,
            WaitForCommit = commitWait,
            TlsOptions = EnableTLS ? new GarnetTlsOptions(
                certFileName: certFile,
                certPassword: certPassword,
                clientCertificateRequired: true,
                certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                issuerCertificatePath: null,
                null, 0, false, null, logger: logger)
            : null,
            DisableObjects = DisableObjects,
            QuietMode = true,
            MetricsSamplingFrequency = metricsSamplingFreq,
            LatencyMonitor = latencyMonitor,
            DeviceFactoryCreator = () => new LocalStorageNamedDeviceFactory(logger: logger),
            AuthSettings = authenticationSettings,
            ExtensionBinPaths = extensionBinPaths,
            ExtensionAllowUnsignedAssemblies = extensionAllowUnsignedAssemblies,
            EnableScatterGatherGet = getSG,
            IndexResizeFrequencySecs = indexResizeFrequencySecs,
            ThreadPoolMinThreads = threadPoolMinThreads,
        };

        if (!string.IsNullOrEmpty(objectStoreTotalMemorySize))
            opts.ObjectStoreTotalMemorySize = objectStoreTotalMemorySize;

        if (indexMaxSize != default) opts.IndexMaxSize = indexMaxSize;
        if (objectStoreIndexMaxSize != default) opts.ObjectStoreIndexMaxSize = objectStoreIndexMaxSize;

        if (lowMemory)
        {
            opts.MemorySize = opts.ObjectStoreLogMemorySize = MemorySize == default ? "512" : MemorySize;
            opts.PageSize = opts.ObjectStorePageSize = PageSize == default ? "512" : PageSize;
        }

        return new GarnetServer(opts);
    }

    public static GarnetServer[] CreateGarnetCluster(
        string checkpointDir,
        EndPointCollection endpoints,
        bool disablePubSub = false,
        bool disableObjects = false,
        bool tryRecover = false,
        bool enableAOF = false,
        int timeout = -1,
        int gossipDelay = 1,
        bool UseTLS = false,
        bool cleanClusterConfig = false,
        bool lowMemory = false,
        string MemorySize = default,
        string PageSize = default,
        string SegmentSize = "1g",
        bool MainMemoryReplication = false,
        string AofMemorySize = "64m",
        bool OnDemandCheckpoint = false,
        int CommitFrequencyMs = 0,
        bool DisableStorageTier = false,
        bool EnableIncrementalSnapshots = false,
        bool FastCommit = false,
        string authUsername = null,
        string authPassword = null,
        bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
        string aclFile = null,
        X509CertificateCollection certificates = null,
        ILoggerFactory loggerFactory = null)
    {
        GarnetServer[] nodes = new GarnetServer[endpoints.Count];
        for (int i = 0; i < nodes.Length; i++)
        {
            IPEndPoint endpoint = (IPEndPoint)endpoints[i];

            GarnetServerOptions opts = GetGarnetServerOptions(
                checkpointDir,
                checkpointDir,
                endpoint.Port,
                disablePubSub,
                disableObjects,
                tryRecover,
                enableAOF,
                timeout,
                gossipDelay,
                UseTLS: UseTLS,
                cleanClusterConfig: cleanClusterConfig,
                lowMemory: lowMemory,
                MemorySize: MemorySize,
                PageSize: PageSize,
                SegmentSize: SegmentSize,
                MainMemoryReplication: MainMemoryReplication,
                AofMemorySize: AofMemorySize,
                OnDemandCheckpoint: OnDemandCheckpoint,
                CommitFrequencyMs: CommitFrequencyMs,
                DisableStorageTier: DisableStorageTier,
                EnableIncrementalSnapshots: EnableIncrementalSnapshots,
                FastCommit: FastCommit,
                authUsername: authUsername,
                authPassword: authPassword,
                useAcl: useAcl,
                aclFile: aclFile,
                certificates: certificates,
                logger: loggerFactory?.CreateLogger("GarnetServer"));

            Assert.IsNotNull(opts);
            int iter = 0;
            while (!IsPortAvailable(opts.Port))
            {
                Assert.Less(30, iter, "Failed to connect within 30 seconds");
                TestContext.Progress.WriteLine($"Waiting for Port {opts.Port} to become available for {TestContext.CurrentContext.WorkerId}:{iter++}");
                Thread.Sleep(1000);
            }
            nodes[i] = new GarnetServer(opts, loggerFactory);
        }
        return nodes;
    }

    public static GarnetServerOptions GetGarnetServerOptions(
        string checkpointDir,
        string logDir,
        int Port,
        bool disablePubSub = false,
        bool disableObjects = false,
        bool tryRecover = false,
        bool enableAOF = false,
        int timeout = -1,
        int gossipDelay = 5,
        bool UseTLS = false,
        bool cleanClusterConfig = false,
        bool lowMemory = false,
        string MemorySize = default,
        string PageSize = default,
        string SegmentSize = "1g",
        bool MainMemoryReplication = false,
        string AofMemorySize = "64m",
        bool OnDemandCheckpoint = false,
        int CommitFrequencyMs = 0,
        bool DisableStorageTier = false,
        bool EnableIncrementalSnapshots = false,
        bool FastCommit = false,
        string authUsername = null,
        string authPassword = null,
        bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
        string aclFile = null,
        X509CertificateCollection certificates = null,
        ILogger logger = null)
    {
        string _LogDir = logDir + $"/{Port}";
        _LogDir = new DirectoryInfo(string.IsNullOrEmpty(_LogDir) ? "." : _LogDir).FullName;

        string _CheckpointDir = checkpointDir + $"/{Port}";
        _CheckpointDir = new DirectoryInfo(string.IsNullOrEmpty(_CheckpointDir) ? "." : _CheckpointDir).FullName;

        IAuthenticationSettings authenticationSettings = null;
        if (useAcl)
        {
            authenticationSettings = new AclAuthenticationSettings(aclFile, authPassword);
        }
        else if (authPassword != null)
        {
            authenticationSettings = new PasswordAuthenticationSettings(authPassword);
        }

        GarnetServerOptions opts = new(logger)
        {
            ThreadPoolMinThreads = 100,
            SegmentSize = SegmentSize,
            ObjectStoreSegmentSize = SegmentSize,
            EnableStorageTier = DisableStorageTier ? false : logDir != null,
            LogDir = DisableStorageTier ? null : _LogDir,
            CheckpointDir = _CheckpointDir,
            Address = Address,
            Port = Port,
            DisablePubSub = disablePubSub,
            DisableObjects = disableObjects,
            Recover = tryRecover,
            IndexSize = "1m",
            ObjectStoreIndexSize = "16k",
            EnableCluster = true,
            CleanClusterConfig = cleanClusterConfig,
            ClusterTimeout = timeout,
            QuietMode = true,
            EnableAOF = enableAOF,
            MemorySize = "1g",
            GossipDelay = gossipDelay,
            EnableFastCommit = FastCommit,
            TlsOptions = UseTLS ? new GarnetTlsOptions(
                certFileName: certFile,
                certPassword: certPassword,
                clientCertificateRequired: true,
                certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                issuerCertificatePath: null,
                null, 0, true, null, null,
                new SslClientAuthenticationOptions
                {
                    ClientCertificates = certificates ?? [new X509Certificate2(certFile, certPassword)],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                },
                logger: logger)
            : null,
            DeviceFactoryCreator = () => new LocalStorageNamedDeviceFactory(logger: logger),
            MainMemoryReplication = MainMemoryReplication,
            AofMemorySize = AofMemorySize,
            OnDemandCheckpoint = OnDemandCheckpoint,
            CommitFrequencyMs = CommitFrequencyMs,
            EnableIncrementalSnapshots = EnableIncrementalSnapshots,
            AuthSettings = useAcl ? authenticationSettings : (authPassword != null ? authenticationSettings : null),
            ClusterUsername = authUsername,
            ClusterPassword = authPassword,
        };

        if (lowMemory)
        {
            opts.MemorySize = opts.ObjectStoreLogMemorySize = MemorySize == default ? "512" : MemorySize;
            opts.PageSize = opts.ObjectStorePageSize = PageSize == default ? "512" : PageSize;
        }

        return opts;
    }

    public static bool IsPortAvailable(int port)
    {
        bool inUse = true;

        IPGlobalProperties ipProperties = IPGlobalProperties.GetIPGlobalProperties();
        IPEndPoint[] ipEndPoints = ipProperties.GetActiveTcpListeners();

        foreach (IPEndPoint endPoint in ipEndPoints)
        {
            if (endPoint.Port == port)
            {
                inUse = false;
                break;
            }
        }

        return inUse;
    }

    /// <summary>
    /// Create config options for SE.Redis client
    /// </summary>
    public static ConfigurationOptions GetConfig(
        EndPointCollection endpoints = default,
        int port = default,
        bool allowAdmin = false,
        bool disablePubSub = false,
        bool useTLS = false,
        string authUsername = null,
        string authPassword = null,
        X509CertificateCollection certificates = null)
    {
        HashSet<string> cmds = RespCommandsInfo.TryGetRespCommandNames(out IReadOnlySet<string> names)
            ? new HashSet<string>(names)
            : new HashSet<string>();

        if (disablePubSub)
        {
            cmds.Remove("SUBSCRIBE");
            cmds.Remove("PUBLISH");
        }

        EndPointCollection defaultEndPoints = endpoints == default ? new() { { Address, port == default ? Port : port }, } : endpoints;
        var configOptions = new ConfigurationOptions
        {
            EndPoints = defaultEndPoints,
            CommandMap = CommandMap.Create(cmds),
            ConnectTimeout = (int)TimeSpan.FromSeconds(2).TotalMilliseconds,
            SyncTimeout = (int)TimeSpan.FromSeconds(30).TotalMilliseconds,
            AsyncTimeout = (int)TimeSpan.FromSeconds(30).TotalMilliseconds,
            AllowAdmin = allowAdmin,
            ReconnectRetryPolicy = new LinearRetry((int)TimeSpan.FromSeconds(10).TotalMilliseconds),
            ConnectRetry = 5,
            IncludeDetailInExceptions = true,
            AbortOnConnectFail = true,
            Password = authPassword,
            User = authUsername,
        };

        if (Debugger.IsAttached)
        {
            configOptions.SyncTimeout = (int)TimeSpan.FromHours(2).TotalMilliseconds;
            configOptions.AsyncTimeout = (int)TimeSpan.FromHours(2).TotalMilliseconds;
        }

        if (useTLS)
        {
            configOptions.Ssl = true;
            configOptions.SslHost = "GarnetTest";
            configOptions.SslClientAuthenticationOptions = (host) =>
            
                new SslClientAuthenticationOptions
                {
                    ClientCertificates = certificates ?? [new X509Certificate2(certFile, certPassword)],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                }
            ;
        }
        return configOptions;
    }

    public static EndPointCollection GetEndPoints(int shards, int port = default)
    {
        Port = port == default ? Port : port;
        EndPointCollection endPoints = [];
        for (int i = 0; i < shards; i++)
            endPoints.Add(IPAddress.Parse("127.0.0.1"), Port + i);
        return endPoints;
    }

    internal static string MethodTestDir => UnitTestWorkingDir();

    /// <summary>
    /// Find root test based on prefix Garnet.Tests
    /// </summary>
    internal static string RootTestsProjectPath =>
        TestContext.CurrentContext.TestDirectory.Split("Garnet.Tests")[0];

    /// <summary>
    /// Build path for unit test working directory using Guid
    /// </summary>
    internal static string UnitTestWorkingDir(string category = null, bool includeGuid = false)
    {
        // Include process id to avoid conflicts between parallel test runs
        string testPath = $"{Environment.ProcessId}_{TestContext.CurrentContext.Test.ClassName}_{TestContext.CurrentContext.Test.MethodName}";
        string rootPath = Path.Combine(RootTestsProjectPath, ".tmp", testPath);

        if (category != null)
            rootPath = Path.Combine(rootPath, category);

        return includeGuid ? Path.Combine(rootPath, Guid.NewGuid().ToString()) : rootPath;
    }

    /// <summary>
    /// Delete a directory recursively
    /// </summary>
    /// <param name="path">The folder to delete</param>
    /// <param name="wait">If true, loop on exceptions that are retryable, and verify the directory no longer exists. Generally true on SetUp, false on TearDown</param>
    internal static void DeleteDirectory(string path, bool wait = false)
    {
        while (true)
        {
            try
            {
                if (!Directory.Exists(path))
                    return;
                foreach (string directory in Directory.GetDirectories(path))
                    DeleteDirectory(directory, wait);
                break;
            }
            catch
            {
            }
        }

        bool retry = true;
        while (retry)
        {
            // Exceptions may happen due to a handle briefly remaining held after Dispose().
            retry = false;
            try
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, true);
            }
            catch (Exception ex) when (ex is IOException ||
                                       ex is UnauthorizedAccessException)
            {
                if (!wait)
                {
                    try { Directory.Delete(path, true); }
                    catch { }
                    return;
                }
                retry = true;
            }
        }
    }

    /// <summary>
    /// Delegate to use in TLS certificate validation
    /// Test certificate should be issued by "CN=Garnet"
    /// </summary>
    public static bool ValidateServerCertificate(
      object sender,
      X509Certificate certificate,
      X509Chain chain,
      SslPolicyErrors sslPolicyErrors)
    {
        if (sslPolicyErrors == SslPolicyErrors.None)
            return true;

        if (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors)
        {
            // Check chain elements
            foreach (X509ChainElement itemInChain in chain.ChainElements)
            {
                if (itemInChain.Certificate.Issuer.Contains("CN=Garnet"))
                    return true;
            }
        }
        throw new Exception($"Certicate errors found {sslPolicyErrors}!");
    }
}