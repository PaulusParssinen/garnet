﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.Common;

namespace Garnet.Server;

internal sealed unsafe partial class RespServerSession
{
    private bool ProcessLatencyCommands(ReadOnlySpan<byte> bufSpan, int count)
    {
        bool errorFlag = false;
        string errorCmd = string.Empty;

        if (count > 0)
        {
            ReadOnlySpan<byte> param = GetCommand(bufSpan, out bool success1);
            if (!success1) return false;

            if (param.SequenceEqual(CmdStrings.HISTOGRAM) || param.SequenceEqual(CmdStrings.histogram))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 1, out bool success))
                {
                    return success;
                }

                byte* ptr = recvBufferPtr + readHead;
                HashSet<LatencyMetricsType> events = null;
                bool invalid = false;
                string invalidEvent = null;
                if (count > 1)
                {
                    events = new();
                    for (int i = 0; i < count - 1; i++)
                    {
                        if (!RespReadUtils.ReadStringWithLengthHeader(out string eventStr, ref ptr, recvBufferPtr + bytesRead))
                            return false;

                        if (Enum.TryParse(eventStr, ignoreCase: true, out LatencyMetricsType eventType))
                        {
                            events.Add(eventType);
                        }
                        else
                        {
                            invalid = true;
                            invalidEvent = eventStr;
                        }
                    }
                }
                else
                    events = GarnetLatencyMetrics.defaultLatencyTypes.ToHashSet();

                if (invalid)
                {
                    while (!RespWriteUtils.WriteError($"ERR Invalid event {invalidEvent}. Try LATENCY HELP", ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    GarnetLatencyMetrics garnetLatencyMetrics = storeWrapper.monitor?.GlobalMetrics.GlobalLatencyMetrics;
                    string response = garnetLatencyMetrics != null ? garnetLatencyMetrics.GetRespHistograms(events) : "*0\r\n";
                    while (!RespWriteUtils.WriteAsciiDirect(response, ref dcurr, dend))
                        SendAndReset();
                }

                readHead = (int)(ptr - recvBufferPtr);
            }
            else if (param.SequenceEqual(CmdStrings.RESET) || param.SequenceEqual(CmdStrings.reset))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 1, out bool success))
                {
                    return success;
                }

                if (count < 1)
                {
                    if (!DrainCommands(bufSpan, count - 1))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param);
                }
                else
                {
                    HashSet<LatencyMetricsType> events = null;
                    byte* ptr = recvBufferPtr + readHead;
                    bool invalid = false;
                    string invalidEvent = null;
                    if (count - 1 > 0)
                    {
                        events = new();
                        for (int i = 0; i < count - 1; i++)
                        {
                            if (!RespReadUtils.ReadStringWithLengthHeader(out string eventStr, ref ptr, recvBufferPtr + bytesRead))
                                return false;

                            if (Enum.TryParse(eventStr, ignoreCase: true, out LatencyMetricsType eventType))
                            {
                                events.Add(eventType);
                            }
                            else
                            {
                                invalid = true;
                                invalidEvent = eventStr;
                            }
                        }
                    }
                    else
                        events = GarnetLatencyMetrics.defaultLatencyTypes.ToHashSet();

                    if (invalid)
                    {
                        while (!RespWriteUtils.WriteError($"ERR Invalid type {invalidEvent}", ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        if (storeWrapper.monitor != null)
                        {
                            foreach (LatencyMetricsType e in events)
                                storeWrapper.monitor.resetLatencyMetrics[e] = true;
                        }
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }

                    readHead = (int)(ptr - recvBufferPtr);
                }
            }
            else if (param.SequenceEqual(CmdStrings.HELP) || param.SequenceEqual(CmdStrings.help))
            {
                byte* ptr = recvBufferPtr + readHead;
                readHead = (int)(ptr - recvBufferPtr);
                List<string> latencyCommands = RespLatencyHelp.GetLatencyCommands();
                while (!RespWriteUtils.WriteArrayLength(latencyCommands.Count, ref dcurr, dend))
                    SendAndReset();
                foreach (string command in latencyCommands)
                {
                    while (!RespWriteUtils.WriteSimpleString(command, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                if (!DrainCommands(bufSpan, count - 1))
                    return false;
                string paramStr = Encoding.ASCII.GetString(param);
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UKNOWN_SUBCOMMAND, ref dcurr, dend))
                    SendAndReset();
            }
        }
        else
        {
            errorFlag = true;
            errorCmd = "LATENCY";
        }

        if (errorFlag && !string.IsNullOrWhiteSpace(errorCmd))
        {
            string errorMsg = string.Format(CmdStrings.GenericErrWrongNumArgs, errorCmd);
            while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                SendAndReset();
        }

        return true;
    }

}