﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;

namespace Garnet.Server;

internal sealed unsafe partial class RespServerSession
{
    private bool ProcessInfoCommand(int count)
    {
        HashSet<InfoMetricsType> sections = null;
        bool invalid = false;
        bool reset = false;
        bool help = false;
        string invalidSection = null;
        if (count > 0)
        {
            byte* ptr = recvBufferPtr + readHead;
            sections = new HashSet<InfoMetricsType>();
            for (int i = 0; i < count; i++)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out string section, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                section = section.ToUpper();
                switch (section)
                {
                    case InfoHelp.RESET:
                        reset = true;
                        break;
                    case InfoHelp.HELP:
                        help = true;
                        break;
                    case InfoHelp.ALL:
                        break;
                    default:
                        if (Enum.TryParse(section, out InfoMetricsType sectionType))
                        {
                            sections.Add(sectionType);
                        }
                        else
                        {
                            invalid = true;
                            invalidSection = section;
                        }
                        break;
                }
            }
            readHead = (int)(ptr - recvBufferPtr);
        }

        if (invalid)
        {
            while (!RespWriteUtils.WriteError($"ERR Invalid section {invalidSection}. Try INFO HELP", ref dcurr, dend))
                SendAndReset();
            return true;
        }

        if (help)
        {
            GetHelpMessage();
        }
        else if (reset)
        {
            if (storeWrapper.monitor != null)
                storeWrapper.monitor.resetEventFlags[InfoMetricsType.STATS] = true;
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
        }
        else
        {
            InfoMetricsType[] sectionsArr = sections == null ? GarnetInfoMetrics.defaultInfo : sections.ToArray();
            GarnetInfoMetrics garnetInfo = new();
            string info = garnetInfo.GetRespInfo(sectionsArr, storeWrapper);
            while (!RespWriteUtils.WriteAsciiBulkString(info, ref dcurr, dend))
                SendAndReset();
        }
        return true;

    }

    private void GetHelpMessage()
    {
        List<string> sectionsHelp = InfoHelp.GetInfoTypeHelpMessage();
        while (!RespWriteUtils.WriteArrayLength(sectionsHelp.Count, ref dcurr, dend))
            SendAndReset();
        foreach (string sectionInfo in sectionsHelp)
        {
            while (!RespWriteUtils.WriteAsciiBulkString(sectionInfo, ref dcurr, dend))
                SendAndReset();
        }
    }
}