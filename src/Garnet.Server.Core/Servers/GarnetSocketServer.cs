// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using Garnet.Common;
using Garnet.networking;
using Garnet.Server.TLS;
using Microsoft.Extensions.Logging;

namespace Garnet.Server
{
    /// <summary>
    /// A Garnet Server built-on top of <see cref="Socket"/>
    /// </summary>
    public class GarnetSocketServer : IGarnetServer, IServerHook
    {
        readonly SocketAsyncEventArgs acceptEventArg;
        readonly Socket servSocket;
        readonly IGarnetTlsOptions tlsOptions;
        readonly int networkSendThrottleMax;
        readonly LimitedFixedBufferPool networkPool;

        /// <summary>
        /// Active network handlers
        /// </summary>
        protected readonly ConcurrentDictionary<INetworkHandler, byte> activeHandlers;

        /// <summary>
        /// Count of active network handlers sessions
        /// </summary>
        protected int activeHandlerCount;

        /// <summary>
        /// Session providers
        /// </summary>
        readonly ConcurrentDictionary<WireFormat, ISessionProvider> sessionProviders;

        /// <summary>
        /// Server Address
        /// </summary>        
        public string Address { get; }

        /// <summary>
        /// Server Port
        /// </summary>
        public int Port { get; }

        /// <summary>
        /// Server NetworkBufferSize
        /// </summary>        
        public int NetworkBufferSize { get; }

        /// <summary>
        /// Check if server has been disposed
        /// </summary>
        public bool Disposed { get; set; }

        /// <summary>
        /// Logger
        /// </summary>
        protected readonly ILogger logger;

        private long totalConnectionReceived = 0;
        private long totalConnectionsDisposed = 0;

        /// <summary>
        /// Get total_connections_received
        /// </summary>
        public long ConnectionsReceived => totalConnectionReceived;

        /// <summary>
        /// Get total_connections_disposed
        /// </summary>
        public long ConnectionsDiposed => totalConnectionsDisposed;

        /// <summary>
        /// Creates the <see cref="GarnetSocketServer"
        /// </summary>
        public GarnetSocketServer(
            EndPoint endpoint,
            int networkBufferSize = default,
            IGarnetTlsOptions tlsOptions = null,
            int networkSendThrottleMax = 8,
            int connectionBacklog = 512,
            ILogger logger = null)
        {

            Address = address;
            Port = port;
            networkPool = new LimitedFixedBufferPool(BufferSizeUtils.ServerBufferSize(new MaxSizeSettings()), logger: logger);
            var ip = string.IsNullOrEmpty(Address) ? IPAddress.Any : IPAddress.Parse(Address);
            servSocket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArg_Completed;

            NetworkBufferSize = networkBufferSize;
            if (networkBufferSize == default)
                NetworkBufferSize = BufferSizeUtils.ClientBufferSize(new MaxSizeSettings());

            logger = logger == null ? null : new SessionLogger(logger, $"[{address ?? StoreWrapper.GetIp()}:{port}] ");

            activeHandlers = new();
            sessionProviders = new();
            activeHandlerCount = 0;
            Disposed = false;

        }

        /// <summary>
        /// Add to total_connections_received
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementConnectionReceived() => Interlocked.Increment(ref totalConnectionReceived);

        /// <summary>
        /// Add to total_connections_disposed
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementConnectionDisposed() => Interlocked.Increment(ref totalConnectionsDisposed);

        /// <summary>
        /// Reset connection recv counter. Multiplier for accounting for pub/sub
        /// </summary>
        public void ResetConnectionsReceived() => Interlocked.Exchange(ref totalConnectionReceived, activeHandlers.Count);

        /// <summary>
        /// Reset connection disposed counter
        /// </summary>
        public void ResetConnectionsDisposed() => Interlocked.Exchange(ref totalConnectionsDisposed, 0);

        /// <summary>
        /// Get active consumers
        /// </summary>
        public IEnumerable<IMessageConsumer> ActiveConsumers()
        {
            foreach (var kvp in activeHandlers)
            {
                var consumer = kvp.Key.Session;
                if (consumer != null)
                    yield return consumer;
            }
        }

        /// <summary>
        /// Get active consumers
        /// </summary>
        public IEnumerable<IClusterSession> ActiveClusterSessions()
        {
            foreach (var kvp in activeHandlers)
            {
                var consumer = kvp.Key.Session;
                if (consumer != null)
                    yield return ((RespServerSession)consumer).clusterSession;
            }
        }

        /// <inheritdoc />
        public void Register(WireFormat wireFormat, ISessionProvider backendProvider)
        {
            if (!sessionProviders.TryAdd(wireFormat, backendProvider))
                throw new GarnetException($"Wire format {wireFormat} already registered");
        }

        /// <inheritdoc />
        public void Unregister(WireFormat wireFormat, out ISessionProvider provider)
            => sessionProviders.TryRemove(wireFormat, out provider);

        /// <inheritdoc />
        public ConcurrentDictionary<WireFormat, ISessionProvider> GetSessionProviders() => sessionProviders;

        /// <inheritdoc />
        public bool AddSession(WireFormat protocol, ref ISessionProvider provider, INetworkSender networkSender, out IMessageConsumer session)
        {
            session = provider.GetSession(protocol, networkSender);
            return true;
        }

        /// <inheritdoc />
        public virtual void Dispose()
        {
            Disposed = true;
            DisposeActiveHandlers();
            sessionProviders.Clear();
        }

        internal void DisposeActiveHandlers()
        {
            logger?.LogTrace("Begin disposing active handlers");
#if HANGDETECT
            int count = 0;
#endif
            while (activeHandlerCount >= 0)
            {
                while (activeHandlerCount > 0)
                {
                    foreach (var kvp in activeHandlers)
                    {
                        var _handler = kvp.Key;
                        _handler?.Dispose();
                    }
                    Thread.Yield();
#if HANGDETECT
                    if (++count % 10000 == 0)
                        logger?.LogTrace("Dispose iteration {count}, {activeHandlerCount}", count, activeHandlerCount);
#endif
                }
                if (Interlocked.CompareExchange(ref activeHandlerCount, int.MinValue, 0) == 0)
                    break;
            }
            logger?.LogTrace("End disposing active handlers");
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            servSocket.Dispose();
            acceptEventArg.UserToken = null;
            acceptEventArg.Dispose();
            networkPool.Dispose();
        }

        /// <summary>
        /// Start listening to incoming requests
        /// </summary>
        public void Start()
        {
            var ip = Address == null ? IPAddress.Any : IPAddress.Parse(Address);
            var endPoint = new IPEndPoint(ip, Port);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);
            if (!servSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
        }

        private void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                do
                {
                    if (!HandleNewConnection(e)) break;
                    e.AcceptSocket = null;
                } while (!servSocket.AcceptAsync(e));
            }
            // socket disposed
            catch (ObjectDisposedException) { }
        }

        private unsafe bool HandleNewConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                return false;
            }

            e.AcceptSocket.NoDelay = true;

            // Ok to create new event args on accept because we assume a connection to be long-running
            string remoteEndpointName = e.AcceptSocket.RemoteEndPoint?.ToString();
            logger?.LogDebug("Accepted TCP connection from {remoteEndpoint}", remoteEndpointName);


            ServerTcpNetworkHandler handler = null;
            if (activeHandlerCount >= 0)
            {
                if (Interlocked.Increment(ref activeHandlerCount) > 0)
                {
                    try
                    {
                        handler = new ServerTcpNetworkHandler(this, e.AcceptSocket, networkPool, tlsOptions != null, networkSendThrottleMax, logger);
                        if (!activeHandlers.TryAdd(handler, default))
                            throw new Exception("Unable to add handler to dictionary");

                        handler.Start(tlsOptions?.TlsServerOptions, remoteEndpointName);
                        IncrementConnectionReceived();
                        return true;
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Error starting network handler");
                        Interlocked.Decrement(ref activeHandlerCount);
                        handler?.Dispose();
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// Create session (message consumer) given incoming bytes
        /// </summary>
        /// <param name="bytes"></param>
        /// <param name="networkSender"></param>
        /// <param name="session"></param>
        /// <returns></returns>
        public bool TryCreateMessageConsumer(Span<byte> bytes, INetworkSender networkSender, out IMessageConsumer session)
        {
            session = null;

            // We need at least 4 bytes to determine session            
            if (bytes.Length < 4)
                return false;

            WireFormat protocol = WireFormat.ASCII;

            if (!GetSessionProviders().TryGetValue(protocol, out var provider))
            {
                var input = System.Text.Encoding.ASCII.GetString(bytes);
                logger?.LogError("Cannot identify wire protocol {bytes}", input);
                throw new Exception($"Unsupported incoming wire format {protocol} {input}");
            }

            if (!AddSession(protocol, ref provider, networkSender, out session))
                throw new Exception($"Unable to add session");

            return true;
        }

        /// <inheritdoc />
        public void DisposeMessageConsumer(INetworkHandler session)
        {
            if (activeHandlers.TryRemove(session, out _))
            {
                Interlocked.Decrement(ref activeHandlerCount);
                IncrementConnectionDisposed();
                try
                {
                    session.Session?.Dispose();
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Error disposing RespServerSession");
                }
            }
        }
    }
}