// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Garnet.Server;

internal sealed class SystemMetrics
{
    [DllImport("psapi.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool GetPerformanceInfo([Out] out PerformanceInformation PerformanceInformation, [In] int Size);

    [StructLayout(LayoutKind.Sequential)]
    public struct PerformanceInformation
    {
        public int Size;
        public IntPtr CommitTotal;
        public IntPtr CommitLimit;
        public IntPtr CommitPeak;
        public IntPtr PhysicalTotal;
        public IntPtr PhysicalAvailable;
        public IntPtr SystemCache;
        public IntPtr KernelTotal;
        public IntPtr KernelPaged;
        public IntPtr KernelNonPaged;
        public IntPtr PageSize;
        public int HandlesCount;
        public int ProcessCount;
        public int ThreadCount;
    }

    public static long GetTotalMemory(long units = 1)
    {
        if (Environment.OSVersion.Platform == PlatformID.Win32NT)
        {
            PerformanceInformation pi = new PerformanceInformation();
            if (GetPerformanceInfo(out pi, Marshal.SizeOf(pi)))
            {
                return Convert.ToInt64((pi.PhysicalTotal.ToInt64() * pi.PageSize.ToInt64() / units));
            }
            else
            {
                return -1;
            }
        }
        else
        {
            GCMemoryInfo gcMemoryInfo = GC.GetGCMemoryInfo();
            return gcMemoryInfo.TotalAvailableMemoryBytes / units;
        }
    }

    public static long GetPhysicalAvailableMemory(long units = 1)
    {
        if (Environment.OSVersion.Platform == PlatformID.Win32NT)
        {
            PerformanceInformation pi = new PerformanceInformation();
            if (GetPerformanceInfo(out pi, Marshal.SizeOf(pi)))
            {
                return Convert.ToInt64((pi.PhysicalAvailable.ToInt64() * pi.PageSize.ToInt64() / units));
            }
            else
            {
                return -1;
            }
        }
        else
        {
            var cproc = Process.GetCurrentProcess();
            return -1;
        }
    }

    public static long GetPagedMemorySize(long units = 1)
    {
        var cproc = Process.GetCurrentProcess();
        return cproc.PagedMemorySize64 / units;
    }

    public static long GetPagedSystemMemorySize(long units = 1)
    {
        var cproc = Process.GetCurrentProcess();
        return cproc.PagedSystemMemorySize64 / units;
    }

    public static long GetPeakPagedMemorySize(long units = 1)
    {
        var cproc = Process.GetCurrentProcess();
        return cproc.PeakPagedMemorySize64 / units;
    }

    public static long GetVirtualMemorySize64(long units = 1)
    {
        var cproc = Process.GetCurrentProcess();
        return cproc.VirtualMemorySize64 / units;
    }

    public static long GetPrivateMemorySize64(long units = 1)
    {
        var cproc = Process.GetCurrentProcess();
        return cproc.PrivateMemorySize64 / units;
    }

    public static long GetPeakVirtualMemorySize64(long units = 1)
    {
        var cproc = Process.GetCurrentProcess();
        return cproc.PeakVirtualMemorySize64 / units;
    }

    public static long GetPhysicalMemoryUsage(long units = 1)
    {
        var cproc = Process.GetCurrentProcess();
        return cproc.WorkingSet64 / units;
    }

    public static long GetPeakPhysicalMemoryUsage(long units = 1)
    {
        var cproc = Process.GetCurrentProcess();
        return cproc.PeakWorkingSet64 / units;
    }
}