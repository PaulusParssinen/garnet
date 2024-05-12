// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.Common;

namespace Garnet.Server;

/// <summary>
/// Logic for parsing command info from RESP format
/// </summary>
public class RespCommandInfoParser
{
    /// <summary>
    /// Tries to parse a RespCommandInfo object from RESP format
    /// </summary>
    /// <param name="ptr">Pointer to current RESP chunk to read</param>
    /// <param name="end">Pointer to end of RESP chunk to read</param>
    /// <param name="supportedCommands">Mapping between command name and Garnet RespCommand and ArrayCommand values</param>
    /// <param name="commandInfo">Parsed RespCommandsInfo object</param>
    /// <param name="parentCommand">Name of parent command, null if none</param>
    /// <returns>True if parsing successful</returns>
    public static unsafe bool TryReadFromResp(ref byte* ptr, byte* end, IReadOnlyDictionary<string, (RespCommand, byte?)> supportedCommands, out RespCommandsInfo commandInfo, string parentCommand = null)
    {
        commandInfo = default;

        // Command info is null
        if (new ReadOnlySpan<byte>(ptr, 5).SequenceEqual("$-1\r\n"u8)) return true;

        // Verify command info array length
        if (!RespReadUtils.ReadArrayLength(out int infoElemCount, ref ptr, end)
            || infoElemCount != 10) return false;

        // 1) Name
        if (!RespReadUtils.ReadStringWithLengthHeader(out string name, ref ptr, end)) return false;

        // 2) Arity
        if (!RespReadUtils.ReadIntegerAsString(out string strArity, ref ptr, end)
            || !int.TryParse(strArity, out int arity)) return false;

        // 3) Flags
        RespCommandFlags flags = RespCommandFlags.None;
        if (!RespReadUtils.ReadArrayLength(out int flagCount, ref ptr, end)) return false;
        for (int flagIdx = 0; flagIdx < flagCount; flagIdx++)
        {
            if (!RespReadUtils.ReadSimpleString(out string strFlag, ref ptr, end)
                || !EnumUtils.TryParseEnumFromDescription<RespCommandFlags>(strFlag, out RespCommandFlags flag))
                return false;
            flags |= flag;
        }

        // 4) First key
        if (!RespReadUtils.ReadIntegerAsString(out string strFirstKey, ref ptr, end)
            || !int.TryParse(strFirstKey, out int firstKey)) return false;

        // 5) Last key
        if (!RespReadUtils.ReadIntegerAsString(out string strLastKey, ref ptr, end)
            || !int.TryParse(strLastKey, out int lastKey)) return false;

        // 6) Step
        if (!RespReadUtils.ReadIntegerAsString(out string strStep, ref ptr, end)
            || !int.TryParse(strStep, out int step)) return false;

        // 7) ACL categories
        RespAclCategories aclCategories = RespAclCategories.None;
        if (!RespReadUtils.ReadArrayLength(out int aclCatCount, ref ptr, end)) return false;
        for (int aclCatIdx = 0; aclCatIdx < aclCatCount; aclCatIdx++)
        {
            if (!RespReadUtils.ReadSimpleString(out string strAclCat, ref ptr, end)
                || !EnumUtils.TryParseEnumFromDescription<RespAclCategories>(strAclCat.TrimStart('@'), out RespAclCategories aclCat))
                return false;
            aclCategories |= aclCat;
        }

        // 8) Tips
        if (!RespReadUtils.ReadStringArrayWithLengthHeader(out string[] tips, ref ptr, end)) return false;

        // 9) Key specifications
        if (!RespReadUtils.ReadArrayLength(out int ksCount, ref ptr, end)) return false;
        var keySpecifications = new RespCommandKeySpecification[ksCount];
        for (int ksIdx = 0; ksIdx < ksCount; ksIdx++)
        {
            if (!RespKeySpecificationParser.TryReadFromResp(ref ptr, end, out RespCommandKeySpecification keySpec)) return false;
            keySpecifications[ksIdx] = keySpec;
        }

        // 10) SubCommands
        if (!RespReadUtils.ReadArrayLength(out int scCount, ref ptr, end)) return false;
        var subCommands = new List<RespCommandsInfo>();
        for (int scIdx = 0; scIdx < scCount; scIdx++)
        {
            if (!TryReadFromResp(ref ptr, end, supportedCommands, out commandInfo, name))
                return false;

            subCommands.Add(commandInfo);
        }

        commandInfo = new RespCommandsInfo()
        {
            Command = supportedCommands[parentCommand ?? name].Item1,
            ArrayCommand = supportedCommands[parentCommand ?? name].Item2,
            Name = name.ToUpper(),
            Arity = arity,
            Flags = flags,
            FirstKey = firstKey,
            LastKey = lastKey,
            Step = step,
            AclCategories = aclCategories,
            Tips = tips.Length == 0 ? null : tips,
            KeySpecifications = keySpecifications.Length == 0 ? null : keySpecifications,
            SubCommands = subCommands.Count == 0 ? null : subCommands.OrderBy(sc => sc.Name).ToArray()
        };

        return true;
    }
}

/// <summary>
/// Logic for parsing key specification from RESP format
/// </summary>
internal class RespKeySpecificationParser
{
    /// <summary>
    /// Tries to parse RespCommandKeySpecification from RESP format
    /// </summary>
    /// <param name="ptr">Pointer to current RESP chunk to read</param>
    /// <param name="end">Pointer to end of RESP chunk to read</param>
    /// <param name="keySpec">Parsed RespCommandKeySpecification object</param>
    /// <returns>True if parsing successful</returns>
    internal static unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out RespCommandKeySpecification keySpec)
    {
        keySpec = default;

        string notes = null;
        KeySpecificationFlags flags = KeySpecificationFlags.None;
        KeySpecMethodBase beginSearch = null;
        KeySpecMethodBase findKeys = null;

        if (!RespReadUtils.ReadArrayLength(out int elemCount, ref ptr, end)) return false;

        for (int elemIdx = 0; elemIdx < elemCount; elemIdx += 2)
        {
            if (!RespReadUtils.ReadStringWithLengthHeader(out string ksKey, ref ptr, end)) return false;

            if (string.Equals(ksKey, "notes", StringComparison.Ordinal))
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out notes, ref ptr, end)) return false;
            }
            else if (string.Equals(ksKey, "flags", StringComparison.Ordinal))
            {
                if (!RespReadUtils.ReadArrayLength(out int flagsCount, ref ptr, end)) return false;
                for (int flagIdx = 0; flagIdx < flagsCount; flagIdx++)
                {
                    if (!RespReadUtils.ReadSimpleString(out string strFlag, ref ptr, end)
                        || !EnumUtils.TryParseEnumFromDescription<KeySpecificationFlags>(strFlag, out KeySpecificationFlags flag))
                        return false;
                    flags |= flag;
                }
            }
            else if (string.Equals(ksKey, "begin_search", StringComparison.Ordinal))
            {
                if (!RespKeySpecificationTypesParser.TryReadFromResp(ksKey, ref ptr, end, out beginSearch)) return false;
            }
            else if (string.Equals(ksKey, "find_keys", StringComparison.Ordinal))
            {
                if (!RespKeySpecificationTypesParser.TryReadFromResp(ksKey, ref ptr, end, out findKeys)) return false;
            }
            else
            {
                return false;
            }
        }

        keySpec = new RespCommandKeySpecification()
        {
            Notes = notes,
            Flags = flags,
            BeginSearch = beginSearch,
            FindKeys = findKeys
        };

        return true;
    }
}

/// <summary>
/// Logic for parsing BeginSearch / FindKeys key specification from RESP format
/// </summary>
internal class RespKeySpecificationTypesParser
{
    /// <summary>
    /// Tries to parse KeySpecMethodBase from RESP format
    /// </summary>
    /// <param name="keySpecKey">Type of key specification ("begin_search" / "find_keys")</param>
    /// <param name="ptr">Pointer to current RESP chunk to read</param>
    /// <param name="end">Pointer to end of RESP chunk to read</param>
    /// <param name="keySpecMethod">Parsed KeySpecMethodBase object</param>
    /// <returns>True if parsing successful</returns>
    public static unsafe bool TryReadFromResp(string keySpecKey, ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
    {
        keySpecMethod = default;

        if (!TryReadKeySpecHeader(ref ptr, end, out string keySpecType)) return false;

        IKeySpecParser parser;
        if (string.Equals(keySpecKey, "begin_search", StringComparison.Ordinal))
        {
            if (string.Equals(keySpecType, "index", StringComparison.Ordinal))
                parser = BeginSearchIndexParser.Instance;
            else if (string.Equals(keySpecType, "keyword", StringComparison.Ordinal))
                parser = BeginSearchKeywordParser.Instance;
            else if (string.Equals(keySpecType, "unknown", StringComparison.Ordinal))
                parser = BeginSearchUnknownParser.Instance;
            else return false;
        }
        else if (string.Equals(keySpecKey, "find_keys", StringComparison.Ordinal))
        {
            if (string.Equals(keySpecType, "range", StringComparison.Ordinal))
                parser = FindKeysRangeParser.Instance;
            else if (string.Equals(keySpecType, "keynum", StringComparison.Ordinal))
                parser = FindKeysKeyNumParser.Instance;
            else if (string.Equals(keySpecType, "unknown", StringComparison.Ordinal))
                parser = FindKeysUnknownParser.Instance;
            else return false;
        }
        else return false;

        if (!parser.TryReadFromResp(ref ptr, end, out keySpecMethod)) return false;

        return true;
    }

    /// <summary>
    /// Tries to parse key spec header from RESP format
    /// </summary>
    /// <param name="ptr">Pointer to current RESP chunk to read</param>
    /// <param name="end">Pointer to end of RESP chunk to read</param>
    /// <param name="keySpecType">Parsed key spec type</param>
    /// <returns>True if parsing successful</returns>
    private static unsafe bool TryReadKeySpecHeader(ref byte* ptr, byte* end, out string keySpecType)
    {
        keySpecType = default;

        if (!RespReadUtils.ReadArrayLength(out int ksTypeElemCount, ref ptr, end)
            || ksTypeElemCount != 4
            || !RespReadUtils.ReadStringWithLengthHeader(out string ksTypeStr, ref ptr, end)
            || !string.Equals(ksTypeStr, "type", StringComparison.Ordinal)
            || !RespReadUtils.ReadStringWithLengthHeader(out string ksType, ref ptr, end)
            || !RespReadUtils.ReadStringWithLengthHeader(out string ksSpecStr, ref ptr, end)
            || !string.Equals(ksSpecStr, "spec", StringComparison.Ordinal)) return false;

        keySpecType = ksType;
        return true;
    }

    /// <summary>
    /// Interface for classes implementing parsing of KeySpecMethodBase objects
    /// </summary>
    internal interface IKeySpecParser
    {
        /// <summary>
        /// Tries to parse KeySpecMethodBase from RESP format
        /// </summary>
        /// <param name="ptr">Pointer to current RESP chunk to read</param>
        /// <param name="end">Pointer to end of RESP chunk to read</param>
        /// <param name="keySpecMethod">Parsed KeySpecMethodBase object</param>
        /// <returns>True if parsing successful</returns>
        unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod);
    }

    /// <summary>
    /// Parser for the BeginSearchIndex key specification method
    /// </summary>
    internal sealed class BeginSearchIndexParser : IKeySpecParser
    {
        private static BeginSearchIndexParser ParserInstance;

        /// <summary>
        /// Disallow default constructor (singleton)
        /// </summary>
        private BeginSearchIndexParser() { }

        /// <summary>
        /// Returns the singleton instance of <see cref="BeginSearchIndexParser" />.
        /// </summary>
        public static BeginSearchIndexParser Instance
        {
            get { return ParserInstance ??= new BeginSearchIndexParser(); }
        }

        /// <inheritdoc />
        public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
        {
            keySpecMethod = default;

            if (!RespReadUtils.ReadArrayLength(out int ksSpecElemCount, ref ptr, end)
                || ksSpecElemCount != 2
                || !RespReadUtils.ReadStringWithLengthHeader(out string ksArgKey, ref ptr, end)
                || !string.Equals(ksArgKey, "index", StringComparison.Ordinal)
                || !RespReadUtils.ReadIntegerAsString(out string strIndex, ref ptr, end)
                || !int.TryParse(strIndex, out int index)) return false;

            keySpecMethod = new BeginSearchIndex(index);

            return true;
        }
    }

    /// <summary>
    /// Parser for the BeginSearchKeyword key specification method
    /// </summary>

    internal sealed class BeginSearchKeywordParser : IKeySpecParser
    {
        private static BeginSearchKeywordParser ParserInstance;

        /// <summary>
        /// Disallow default constructor (singleton)
        /// </summary>
        private BeginSearchKeywordParser() { }

        /// <summary>
        /// Returns the singleton instance of <see cref="BeginSearchKeywordParser" />.
        /// </summary>
        public static BeginSearchKeywordParser Instance
        {
            get { return ParserInstance ??= new BeginSearchKeywordParser(); }
        }

        /// <inheritdoc />
        public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
        {
            keySpecMethod = default;

            if (!RespReadUtils.ReadArrayLength(out int specElemCount, ref ptr, end)
                || specElemCount != 4
                || !RespReadUtils.ReadStringWithLengthHeader(out string argKey, ref ptr, end)
                || !string.Equals(argKey, "keyword", StringComparison.Ordinal)
                || !RespReadUtils.ReadStringWithLengthHeader(out string keyword, ref ptr, end)
                || !RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end)
                || !string.Equals(argKey, "startfrom", StringComparison.Ordinal)
                || !RespReadUtils.ReadIntegerAsString(out string strStartFrom, ref ptr, end)
                || !int.TryParse(strStartFrom, out int startFrom)) return false;

            keySpecMethod = new BeginSearchKeyword(keyword, startFrom);

            return true;
        }
    }

    /// <summary>
    /// Parser for the BeginSearchUnknown key specification method
    /// </summary>
    internal sealed class BeginSearchUnknownParser : IKeySpecParser
    {
        private static BeginSearchUnknownParser ParserInstance;

        /// <summary>
        /// Disallow default constructor (singleton)
        /// </summary>
        private BeginSearchUnknownParser() { }

        /// <summary>
        /// Returns the singleton instance of <see cref="BeginSearchUnknownParser" />.
        /// </summary>
        public static BeginSearchUnknownParser Instance
        {
            get { return ParserInstance ??= new BeginSearchUnknownParser(); }
        }

        /// <inheritdoc />
        public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
        {
            keySpecMethod = default;

            if (!RespReadUtils.ReadArrayLength(out int ksSpecElemCount, ref ptr, end)
                || ksSpecElemCount == 0) return false;

            keySpecMethod = new BeginSearchUnknown();

            return true;
        }
    }

    /// <summary>
    /// Parser for the FindKeysRange key specification method
    /// </summary>
    internal sealed class FindKeysRangeParser : IKeySpecParser
    {
        private static FindKeysRangeParser ParserInstance;

        /// <summary>
        /// Disallow default constructor (singleton)
        /// </summary>
        private FindKeysRangeParser() { }

        /// <summary>
        /// Returns the singleton instance of <see cref="FindKeysRangeParser" />.
        /// </summary>
        public static FindKeysRangeParser Instance
        {
            get { return ParserInstance ??= new FindKeysRangeParser(); }
        }

        /// <inheritdoc />
        public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
        {
            keySpecMethod = default;

            if (!RespReadUtils.ReadArrayLength(out int specElemCount, ref ptr, end)
                || specElemCount != 6
                || !RespReadUtils.ReadStringWithLengthHeader(out string argKey, ref ptr, end)
                || !string.Equals(argKey, "lastkey", StringComparison.Ordinal)
                || !RespReadUtils.ReadIntegerAsString(out string strLastKey, ref ptr, end)
                || !int.TryParse(strLastKey, out int lastKey)
                || !RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end)
                || !string.Equals(argKey, "keystep", StringComparison.Ordinal)
                || !RespReadUtils.ReadIntegerAsString(out string strKeyStep, ref ptr, end)
                || !int.TryParse(strKeyStep, out int keyStep)
                || !RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end)
                || !string.Equals(argKey, "limit", StringComparison.Ordinal)
                || !RespReadUtils.ReadIntegerAsString(out string strLimit, ref ptr, end)
                || !int.TryParse(strLimit, out int limit)) return false;

            keySpecMethod = new FindKeysRange(lastKey, keyStep, limit);

            return true;
        }
    }

    /// <summary>
    /// Parser for the FindKeysKeyNum key specification method
    /// </summary>
    internal sealed class FindKeysKeyNumParser : IKeySpecParser
    {
        private static FindKeysKeyNumParser ParserInstance;

        /// <summary>
        /// Disallow default constructor (singleton)
        /// </summary>
        private FindKeysKeyNumParser() { }


        /// <summary>
        /// Returns the singleton instance of <see cref="FindKeysKeyNumParser" />.
        /// </summary>
        public static FindKeysKeyNumParser Instance
        {
            get { return ParserInstance ??= new FindKeysKeyNumParser(); }
        }

        /// <inheritdoc />
        public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
        {
            keySpecMethod = default;

            if (!RespReadUtils.ReadArrayLength(out int specElemCount, ref ptr, end)
                || specElemCount != 6
                || !RespReadUtils.ReadStringWithLengthHeader(out string argKey, ref ptr, end)
                || !string.Equals(argKey, "keynumidx", StringComparison.Ordinal)
                || !RespReadUtils.ReadIntegerAsString(out string strKeyNumIdx, ref ptr, end)
                || !int.TryParse(strKeyNumIdx, out int keyNumIdx)
                || !RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end)
                || !string.Equals(argKey, "firstkey", StringComparison.Ordinal)
                || !RespReadUtils.ReadIntegerAsString(out string strFirstKey, ref ptr, end)
                || !int.TryParse(strFirstKey, out int firstKey)
                || !RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end)
                || !string.Equals(argKey, "keystep", StringComparison.Ordinal)
                || !RespReadUtils.ReadIntegerAsString(out string strKeyStep, ref ptr, end)
                || !int.TryParse(strKeyStep, out int keyStep)) return false;

            keySpecMethod = new FindKeysKeyNum(keyNumIdx, firstKey, keyStep);

            return true;
        }
    }

    /// <summary>
    /// Parser for the FindKeysUnknown key specification method
    /// </summary>
    internal sealed class FindKeysUnknownParser : IKeySpecParser
    {
        private static FindKeysUnknownParser ParserInstance;

        /// <summary>
        /// Disallow default constructor (singleton)
        /// </summary>
        private FindKeysUnknownParser() { }

        /// <summary>
        /// Returns the singleton instance of <see cref="FindKeysUnknownParser" />.
        /// </summary>
        public static FindKeysUnknownParser Instance
        {
            get { return ParserInstance ??= new FindKeysUnknownParser(); }
        }

        /// <inheritdoc />
        public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
        {
            keySpecMethod = default;

            if (!RespReadUtils.ReadArrayLength(out int ksSpecElemCount, ref ptr, end)
                || ksSpecElemCount == 0) return false;

            keySpecMethod = new FindKeysUnknown();

            return true;
        }
    }
}