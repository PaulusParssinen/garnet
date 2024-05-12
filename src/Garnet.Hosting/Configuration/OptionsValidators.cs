// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Reflection;
using System.Security;
using System.Text;
using System.Text.RegularExpressions;

namespace Garnet;

/// <summary>
/// Basic validation logic for Options property
/// Valid if value is required and has value or if value is not required
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal class OptionValidationAttribute : ValidationAttribute
{
    /// <summary>
    /// Determines if current property is required to have a value
    /// </summary>
    protected readonly bool IsRequired;

    protected static object GetDefault<T>(T t) => default(T);

    internal OptionValidationAttribute(bool isRequired = true)
    {
        IsRequired = isRequired;
    }

    /// <summary>
    /// Checks if current property is valid by checking if value is required and not default or if value is not required
    /// </summary>
    /// <param name="value">Property value to validate</param>
    /// <param name="validationContext">Current validation context</param>
    /// <returns>Validation result</returns>
    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
        if (!IsRequired || value != GetDefault(value) || (value is string strVal && !string.IsNullOrEmpty(strVal)))
            return ValidationResult.Success;

        string baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
        string errorMessage = $"{baseError} Required value was not specified.";
        return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
    }

    /// <summary>
    /// Initial validation logic to check if further validation can be skipped -
    /// Valid result if value is not required value is default
    /// Invalid result if value is not of the expected type
    /// If neither conditions are true, further validation is required
    /// </summary>
    /// <typeparam name="T">Type expected by the validator</typeparam>
    /// <param name="value">Value to validate</param>
    /// <param name="validationContext">Validation context</param>
    /// <param name="validationResult">Validation result - only set if initial validation suffices</param>
    /// <param name="convertedValue">Value converted to type T</param>
    /// <returns>True if further validation can be skipped and validation result is set</returns>
    protected bool TryInitialValidation<T>(object value, ValidationContext validationContext, out ValidationResult validationResult, out T convertedValue)
    {
        validationResult = null;
        convertedValue = default;

        if (!IsRequired && (value == GetDefault(value) || (value is string strVal && string.IsNullOrEmpty(strVal))))
        {
            validationResult = ValidationResult.Success;
            return true;
        }

        if (value is not T tValue)
        {
            string baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
            string errorMessage = $"{baseError} Invalid type. Expected: {typeof(T)}. Actual: {value?.GetType()}";
            validationResult = new ValidationResult(errorMessage, new[] { validationContext.MemberName });
            return true;
        }

        convertedValue = tValue;

        return false;
    }
}

/// <summary>
/// Validation logic for path of type string representing a local directory
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal class DirectoryPathValidationAttribute : OptionValidationAttribute
{
    /// <summary>
    /// Determines if current directory must exist
    /// </summary>
    private readonly bool _mustExist;

    internal DirectoryPathValidationAttribute(bool mustExist, bool isRequired) : base(isRequired)
    {
        _mustExist = mustExist;
    }

    /// <summary>
    /// Directory validation logic, checks access to directory by instantiating a DirectoryInfo object
    /// Invalid if exception thrown during DirectoryInfo instantiation or if directory must exist and doesn't
    /// </summary>
    /// <param name="value">Path to the local directory</param>
    /// <param name="validationContext">Validation context</param>
    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
        if (TryInitialValidation<string>(value, validationContext, out ValidationResult initValidationResult, out string directoryPath))
            return initValidationResult;

        DirectoryInfo directoryInfo;
        string baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;

        try
        {
            directoryInfo = new DirectoryInfo(directoryPath);
        }
        catch (Exception e) when (e is SecurityException or PathTooLongException)
        {
            string errorMessage = $"{baseError} An exception of type {e.GetType()} has occurred while trying to access directory. Directory path: {directoryPath}.";
            return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
        }

        var options = (Options)validationContext.ObjectInstance;

        if (_mustExist && !directoryInfo.Exists)
        {
            string errorMessage = $"{baseError} Specified directory does not exist. Directory path: {directoryPath}.";
            return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
        }

        return ValidationResult.Success;
    }
}

/// <summary>
/// Validation logic for multiple paths of type string representing local directories
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal sealed class DirectoryPathsValidationAttribute : OptionValidationAttribute
{
    /// <summary>
    /// Determines if all directories must exist
    /// </summary>
    private readonly bool _mustExist;

    internal DirectoryPathsValidationAttribute(bool mustExist, bool isRequired) : base(isRequired)
    {
        _mustExist = mustExist;
    }

    /// <summary>
    /// Directories validation logic, calls DirectoryPathValidationAttribute for each directory
    /// </summary>
    /// <param name="value">Paths to the local directories</param>
    /// <param name="validationContext">Validation context</param>
    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
        if (TryInitialValidation<IEnumerable<string>>(value, validationContext, out ValidationResult initValidationResult, out IEnumerable<string> directoryPaths))
            return initValidationResult;

        var errorSb = new StringBuilder();
        bool isValid = true;
        var directoryValidator = new DirectoryPathValidationAttribute(_mustExist, IsRequired);
        foreach (string directoryPath in directoryPaths)
        {
            ValidationResult result = directoryValidator.GetValidationResult(directoryPath, validationContext);
            if (result != null && result != ValidationResult.Success)
            {
                isValid = false;
                errorSb.AppendLine(result.ErrorMessage);
            }
        }

        if (!isValid)
        {
            string errorMessage = $"Error(s) validating one or more directories:{Environment.NewLine}{errorSb}";
            return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
        }

        return ValidationResult.Success;
    }
}

/// <summary>
/// Validation logic for path of type string representing a local file
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal class FilePathValidationAttribute : OptionValidationAttribute
{
    /// <summary>
    /// Determines if current file must exist
    /// </summary>
    private readonly bool _fileMustExist;
    /// <summary>
    /// Determines if current directory must exist
    /// </summary>
    private readonly bool _directoryMustExist;

    /// <summary>
    /// Determines which file extensions are expected for this file
    /// </summary>
    private readonly string[] _acceptedFileExtensions;

    internal FilePathValidationAttribute(bool fileMustExist, bool directoryMustExist, bool isRequired, string[] acceptedFileExtensions = null) : base(isRequired)
    {
        _fileMustExist = fileMustExist;
        _directoryMustExist = directoryMustExist;
        _acceptedFileExtensions = acceptedFileExtensions;
    }

    /// <summary>
    /// File validation logic, checks access to file by instantiating a FileInfo object
    /// Invalid if exception thrown during FileInfo instantiation or if directory must exist and doesn't or if file must exist and doesn't
    /// </summary>
    /// <param name="value">Path to the local file</param>
    /// <param name="validationContext">Validation context</param>
    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
        if (TryInitialValidation<string>(value, validationContext, out ValidationResult initValidationResult, out string filePath))
            return initValidationResult;

        FileInfo fileInfo;
        string baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;

        try
        {
            fileInfo = new FileInfo(filePath);
        }
        catch (Exception e) when (e is SecurityException or UnauthorizedAccessException or NotSupportedException or PathTooLongException)
        {
            string errorMessage = $"{baseError} An exception of type {e.GetType()} has occurred while trying to access file. File path: {filePath}.";
            return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
        }

        var options = (Options)validationContext.ObjectInstance;

        if (_fileMustExist && !fileInfo.Exists)
        {
            string errorMessage = $"{baseError} Specified file does not exist. File path: {filePath}.";
            return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
        }

        if (_directoryMustExist && (fileInfo.Directory == null || !fileInfo.Directory.Exists))
        {
            string errorMessage = $"{baseError} Directory containing specified file does not exist. File path: {filePath}.";
            return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
        }

        if (_acceptedFileExtensions != null && !_acceptedFileExtensions.Any(filePath.EndsWith))
        {
            string errorMessage =
                $"{baseError} Unexpected extension for specified file. Expected: {string.Join(" / ", _acceptedFileExtensions)}.";
            return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
        }

        return ValidationResult.Success;
    }
}

/// <summary>
/// Validation logic for a string representing an IP address (either IPv4 or IPv6)
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal sealed class IpAddressValidationAttribute : OptionValidationAttribute
{
    private const string Localhost = "localhost";

    internal IpAddressValidationAttribute(bool isRequired = true) : base(isRequired)
    {
    }

    /// <summary>
    /// IP validation logic, checks if string matches either IPv4 or IPv6 regex patterns
    /// </summary>
    /// <param name="value">String containing IP address</param>
    /// <param name="validationContext">Validation Logic</param>
    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
        if (TryInitialValidation<string>(value, validationContext, out ValidationResult initValidationResult, out string ipAddress))
            return initValidationResult;

        if (ipAddress.Equals(Localhost, StringComparison.CurrentCultureIgnoreCase) || IPAddress.TryParse(ipAddress, out _))
            return ValidationResult.Success;

        string baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
        string errorMessage = $"{baseError} Expected string in IPv4 / IPv6 format (e.g. 127.0.0.1 / 0:0:0:0:0:0:0:1) or 'localhost'. Actual value: {ipAddress}";
        return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
    }
}

/// <summary>
/// Validation logic for a string representing a memory size (1k, 1kb, 5M, 5Mb, 10g, 10GB etc.)
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal sealed class MemorySizeValidationAttribute : OptionValidationAttribute
{
    private const string MemorySizePattern = @"^\d+([K|k|M|m|G|g][B|b]{0,1})?$";

    internal MemorySizeValidationAttribute(bool isRequired = true) : base(isRequired)
    {
    }

    /// <summary>
    /// Memory size validation logic, checks if string matches memory size regex pattern
    /// </summary>
    /// <param name="value">String containing memory size</param>
    /// <param name="validationContext">Validation context</param>
    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
        if (TryInitialValidation<string>(value, validationContext, out ValidationResult initValidationResult, out string memorySize))
            return initValidationResult;

        if (Regex.IsMatch(memorySize, MemorySizePattern))
            return ValidationResult.Success;

        string baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
        string errorMessage = $"{baseError} Expected string in memory size format (e.g. 1k, 1kb, 10m, 10mb, 50g, 50gb etc). Actual value: {memorySize}";
        return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
    }
}

/// <summary>
/// Validation logic for an integer representing a percentage (range between 0 and 100)
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal sealed class PercentageValidationAttribute : RangeValidationAttribute
{
    internal PercentageValidationAttribute(bool isRequired = true) : base(typeof(int), 0, 100, true, true, isRequired)
    {
    }
}

/// <summary>
/// Validation logic for an object of specified type that implements IComparable, checks if value is contained in a specified range
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal class RangeValidationAttribute : OptionValidationAttribute
{
    // Type of min, max and value to validate
    private readonly Type _rangeType;
    // Range minimum
    private readonly object _min;
    // Range maximum
    private readonly object _max;
    // True if range includes minimum value
    private readonly bool _includeMin;
    // True if range includes maximum value
    private readonly bool _includeMax;

    private readonly MethodInfo _tryInitValidationMethod;

    internal RangeValidationAttribute(Type rangeType, object min, object max, bool includeMin = true, bool includeMax = true, bool isRequired = true) : base(isRequired)
    {
        Type icType = typeof(IComparable<>).MakeGenericType(rangeType);

        if (!rangeType.IsAssignableTo(icType))
            throw new ArgumentException($"rangeType parameter is not assignable to {icType}", nameof(rangeType));
        if (min.GetType() != rangeType)
            throw new ArgumentException($"min parameter is not of type specified by rangeType", nameof(min));
        if (max.GetType() != rangeType)
            throw new ArgumentException($"max parameter is not of type specified by rangeType", nameof(includeMax));

        _rangeType = rangeType;
        _min = min;
        _includeMin = includeMin;
        _max = max;
        _includeMax = includeMax;

        _tryInitValidationMethod = GetType().GetMethod(nameof(TryInitialValidation), BindingFlags.Instance | BindingFlags.NonPublic)
            ?.MakeGenericMethod(_rangeType);
    }

    /// <summary>
    /// Integer validation logic, valid if integer is contained in a specified range
    /// </summary>
    /// <param name="value">Integer value</param>
    /// <param name="validationContext">Validation context</param>
    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
        object[] initValParams = new[] { value, validationContext, null, null };
        if ((bool)_tryInitValidationMethod.Invoke(this, initValParams)!)
        {
            return initValParams[2] as ValidationResult;
        }

        var icVal = value as IComparable;
        int minComp = icVal.CompareTo(_min);
        int maxComp = icVal.CompareTo(_max);
        if ((minComp > 0 || (_includeMin && minComp == 0)) &&
            (maxComp < 0 || (_includeMax && maxComp == 0)))
        {
            return ValidationResult.Success;
        }

        string baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
        string errorMessage = $"{baseError} Expected to be in range {(_includeMin ? "[" : "(")}{_min}, {_max}{(_includeMax ? "]" : ")")}. Actual value: {value}";
        return new ValidationResult(errorMessage, new[] { validationContext.MemberName });
    }
}

/// <summary>
/// Validation logic for an integer, checks if integer is contained in a specified range
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal sealed class IntRangeValidationAttribute : RangeValidationAttribute
{
    internal IntRangeValidationAttribute(int min, int max, bool includeMin = true, bool includeMax = true,
        bool isRequired = true) : base(typeof(int), min, max, includeMin, includeMax, isRequired)
    {
    }
}

/// <summary>
/// Validation logic for an double, checks if double is contained in a specified range
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal sealed class DoubleRangeValidationAttribute : RangeValidationAttribute
{
    internal DoubleRangeValidationAttribute(double min, double max, bool includeMin = true, bool includeMax = true,
        bool isRequired = true) : base(typeof(double), min, max, includeMin, includeMax, isRequired)
    {
    }
}

/// <summary>
/// Validation logic for Log Directory
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal class LogDirValidationAttribute : DirectoryPathValidationAttribute
{
    internal LogDirValidationAttribute(bool mustExist, bool isRequired) : base(mustExist, isRequired)
    {
    }

    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
        var options = (Options)validationContext.ObjectInstance;

        return base.IsValid(value, validationContext);
    }
}

/// <summary>
/// Validation logic for Checkpoint Directory
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal sealed class CheckpointDirValidationAttribute : DirectoryPathValidationAttribute
{
    internal CheckpointDirValidationAttribute(bool mustExist, bool isRequired) : base(mustExist, isRequired)
    {
    }

    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
        var options = (Options)validationContext.ObjectInstance;

        return base.IsValid(value, validationContext);
    }
}

/// <summary>
/// Validation logic for CertFileName
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
internal sealed class CertFileValidationAttribute : FilePathValidationAttribute
{
    internal CertFileValidationAttribute(bool fileMustExist, bool directoryMustExist, bool isRequired) : base(
        fileMustExist, directoryMustExist, isRequired, new[] { ".pfx" })
    {
    }

    /// <summary>
    /// Validation logic for CertFileName, valid if EnableTLS is false in parent Options object
    /// If not, reverts to FilePathValidationAttribute validation
    /// </summary>
    /// <param name="value">Value of CertFileName</param>
    /// <param name="validationContext">Validation context</param>
    /// <returns>Validation result</returns>
    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
        var options = (Options)validationContext.ObjectInstance;
        if (!options.EnableTLS.GetValueOrDefault())
            return ValidationResult.Success;

        return base.IsValid(value, validationContext);
    }
}