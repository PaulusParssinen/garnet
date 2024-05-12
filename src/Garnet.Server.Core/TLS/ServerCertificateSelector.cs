// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;

namespace Garnet.Server.TLS;

/// <summary>
/// Ssl certificate selection to handle certificate refresh
/// </summary>
public sealed class ServerCertificateSelector
{
    /// <summary>
    /// Ssl certificate retry duration in case of failures.
    /// </summary>
    private readonly TimeSpan certificateRefreshRetryInterval = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Ssl certificate subject name.
    /// </summary>
    private readonly string sslCertificateSubjectName;

    /// <summary>
    /// Ssl certificate file name.
    /// </summary>
    private readonly string sslCertificateFileName;

    /// <summary>
    /// Ssl certificate file password
    /// </summary>
    private readonly string sslCertificatePassword;
    private readonly Timer _refreshTimer;
    private readonly ILogger _logger;

    /// <summary>
    /// Ssl certificate retry duration
    /// </summary>
    private readonly TimeSpan certRefreshFrequency;

    /// <summary>
    /// Ssl server certificate.
    /// </summary>
    private X509Certificate2 sslServerCertificate;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServerCertificateSelector"/> class.
    /// </summary>
    public ServerCertificateSelector(string subjectName, int certRefreshFrequencyInSeconds = 0, ILogger logger = null)
    {
        _logger = logger;
        sslCertificateSubjectName = subjectName;

        // First get certificate synchronously on current call
        certRefreshFrequency = TimeSpan.Zero;
        GetServerCertificate(null);

        // Set up future timer, if needed
        // If the sync call failed to fetch certificate, we schedule the first call earlier (after certificateRefreshRetryInterval)
        certRefreshFrequency = TimeSpan.FromSeconds(certRefreshFrequencyInSeconds);
        if (certRefreshFrequency > TimeSpan.Zero)
            _refreshTimer = new Timer(GetServerCertificate, null, sslServerCertificate == null ? certificateRefreshRetryInterval : certRefreshFrequency, certRefreshFrequency);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ServerCertificateSelector"/> class.
    /// </summary>changed th
    public ServerCertificateSelector(string fileName, string filePassword, int certRefreshFrequencyInSeconds = 0, ILogger logger = null)
    {
        _logger = logger;
        sslCertificateFileName = fileName;
        sslCertificatePassword = filePassword;

        // First get certificate synchronously on current call
        certRefreshFrequency = TimeSpan.Zero;
        GetServerCertificate(null);

        // Set up future timer, if needed
        // If the sync call failed to fetch certificate, we schedule the first call earlier (after certificateRefreshRetryInterval)
        certRefreshFrequency = TimeSpan.FromSeconds(certRefreshFrequencyInSeconds);
        if (certRefreshFrequency > TimeSpan.Zero)
            _refreshTimer = new Timer(GetServerCertificate, null, sslServerCertificate == null ? certificateRefreshRetryInterval : certRefreshFrequency, certRefreshFrequency);
    }

    /// <summary>
    /// End refresh timer
    /// </summary>
    public void EndTimer()
    {
        _refreshTimer?.Dispose();
    }

    /// <summary>
    /// Looks up the server certificate for authenticating an HTTPS connection.
    /// </summary>
    /// <returns>The X.509 certificate to use for server authentication.</returns>
    public X509Certificate2 GetSslServerCertificate()
    {
        return sslServerCertificate;
    }

    private void GetServerCertificate(object _)
    {
        try
        {
            if (sslCertificateSubjectName != null)
            {
                sslServerCertificate =
                    CertificateUtils.GetMachineCertificateBySubjectName(
                        sslCertificateSubjectName);
            }
            else
            {
                sslServerCertificate =
                    CertificateUtils.GetMachineCertificateByFile(
                        sslCertificateFileName, sslCertificatePassword);
            }
        }
        catch (Exception ex)
        {
            if (certRefreshFrequency > TimeSpan.Zero)
            {
                _logger?.LogError(ex, $"Unable to fetch certificate. It will be retried after {certificateRefreshRetryInterval}");
                try
                {
                    _refreshTimer?.Change(certificateRefreshRetryInterval, certRefreshFrequency);
                }
                catch (ObjectDisposedException)
                {
                    // Timer is disposed
                }
            }
            else
            {
                // This is not a background timer based call
                _logger?.LogError(ex, "Unable to fetch certificate using the provided filename and password. Make sure you specify a correct CertFileName and CertPassword.");
            }
        }
    }
}