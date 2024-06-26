﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Server;

/// <summary>
/// Enconding and decoding methods for Geospatial
/// </summary>
public static class GeoHash
{
    // Constraints from EPSG:900913 / EPSG:3785 / OSGEO:41001
    private static readonly long geoLongMax = 180;
    private static readonly long geoLongMin = -180;
    private static readonly long geoLatMax = 90;
    private static readonly long geoLatMin = -90;
    private static readonly int precision = 52;

    //Measure based on WGS-84 system
    private static readonly double earthRadiusInMeters = 6372797.560856;

    //The PI/180 constant
    private static readonly double degreesToRadians = 0.017453292519943295769236907684886;

    //The "Geohash alphabet" (32ghs) uses all digits 0-9 and almost all lower case letters except "a", "i", "l" and "o".
    //This table is used for getting the "standard textual representation" of a pair of lat and long.
    private static readonly char[] base32chars = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };


    /// <summary>
    /// Encodes the latitude,longitude coords to a unique 52-bit integer
    /// </summary>
    public static long GeoToLongValue(double latitude, double longitude)
    {
        int i = 0;
        long result = 0;
        bool isLongitudBit = true;

        double[] latitudeRange = new double[] { geoLatMin, geoLatMax };
        double[] longitudeRange = new double[] { geoLongMin, geoLongMax };

        //check for invalid values
        if (!(geoLatMin <= latitude && latitude <= geoLatMax) || !(geoLongMin <= longitude && longitude <= geoLongMax))
            return -1;

        while (i < precision)
        {
            Encode(isLongitudBit ? longitude : latitude, isLongitudBit ? longitudeRange : latitudeRange, ref result);
            isLongitudBit = !isLongitudBit;
            i++;
        }

        return result;
    }

    /// <summary>
    /// Gets pair (latitude, longitude) of GPS coordinates
    /// latitude comes before longitude in the ISO 6709 standard
    /// https://en.wikipedia.org/wiki/ISO_6709#Order,_sign,_and_units
    /// Latitude refers to the Y-values and are between -90 and +90 degrees.
    /// Longitude refers to the X-coordinates and are between -180 and +180 degrees.
    /// </summary>
    /// <returns>(latitude, longitude)</returns>
    public static (double, double) GetCoordinatesFromLong(long longValue)
    {
        string binaryString = Convert.ToString(longValue, 2);

        while (binaryString.Length < precision)
        {
            binaryString = "0" + binaryString;
        }

        bool isLongitudBit = true;

        double[] latitudeRange = new double[] { geoLatMin, geoLatMax };
        double[] longitudeRange = new double[] { geoLongMin, geoLongMax };

        for (int i = 0; i < precision; i++)
        {
            Decode(isLongitudBit ? longitudeRange : latitudeRange, binaryString[i] != '0');
            isLongitudBit = !isLongitudBit;
        }

        double latitude = (latitudeRange[0] + latitudeRange[1]) / 2;
        double longitude = (longitudeRange[0] + longitudeRange[1]) / 2;
        return (latitude, longitude);
    }

    /// <summary>
    /// Gets the base32 value
    /// </summary>
    /// <returns>The GeoHash representation of the 52bit</returns>
    public static string GetGeoHashCode(long longEncodedValue)
    {
        // Length for the GeoHash
        int codeLength = 11;

        string result = string.Empty;
        bool isLongitudBit = true;
        long hashValue = 0;

        double[] latitudeRange = new double[] { geoLatMin, geoLatMax };
        double[] longitudeRange = new double[] { geoLongMin, geoLongMax };

        double latitude;
        double longitude;

        (latitude, longitude) = GetCoordinatesFromLong(longEncodedValue);

        // check for invalid values
        if (!(geoLatMin <= latitude && latitude <= geoLatMax) || !(geoLongMin <= longitude && longitude <= geoLongMax))
            return null;

        int bits = 0;

        while (result.Length < codeLength)
        {
            Encode(isLongitudBit ? longitude : latitude, isLongitudBit ? longitudeRange : latitudeRange, ref hashValue);
            isLongitudBit = !isLongitudBit;
            bits++;
            if (bits != 5)
            {
                continue;
            }
            char code = base32chars[hashValue];
            result += code;
            bits = 0;
            hashValue = 0;
        }

        return result;
    }

    /// <summary>
    /// Gets the distance in meters using Haversine Formula
    /// https://en.wikipedia.org/wiki/Haversine_formula
    /// </summary>
    public static double Distance(double sourceLat, double sourceLon, double targetLat, double targetLon)
    {
        // Convert to Radians
        //Multiply by Math.PI / 180 to convert degrees to radians.
        double lonRad = (sourceLon - targetLon) * degreesToRadians;
        double latRad = (sourceLat - targetLat) * degreesToRadians;
        double latHaversine = Math.Pow(Math.Sin(latRad * 0.5), 2);
        double lonHaversine = Math.Pow(Math.Sin(lonRad * 0.5), 2);

        double tmp = Math.Cos(sourceLat * degreesToRadians) * Math.Cos(targetLat * degreesToRadians);

        return 2 * Math.Asin(Math.Sqrt(latHaversine + tmp * lonHaversine)) * earthRadiusInMeters;
    }


    /// <summary>
    /// Find if a point is in the axis-aligned rectangle.
    /// when the distance between the searched point and the center point is less than or equal to 
    /// height/2 or width/2,
    /// the point is in the rectangle.
    /// </summary>
    public static bool GetDistanceWhenInRectangle(double widthMts, double heightMts, double latCenterPoint, double lonCenterPoint, double lat2, double lon2, ref double distance)
    {
        double lon_distance = Distance(lat2, lon2, latCenterPoint, lon2);
        double lat_distance = Distance(lat2, lon2, lat2, lonCenterPoint);
        if (lon_distance > widthMts / 2 || lat_distance > heightMts / 2)
        {
            return false;
        }

        distance = Distance(latCenterPoint, lonCenterPoint, lat2, lon2);
        return true;
    }

    private static void Encode(double value, double[] range, ref long result)
    {
        double mid = (range[0] + range[1]) / 2;
        int idx = value > mid ? 0 : 1;
        range[idx] = mid;
        result = (result << 1) + (value > mid ? 1 : 0);
    }

    private static void Decode(double[] range, bool isOnBit)
    {
        double mid = (range[0] + range[1]) / 2;
        int idx = isOnBit ? 0 : 1;
        range[idx] = mid;
    }

    /// <summary>
    /// 
    /// </summary>
    public static double ConvertValueToMeters(double value, byte[] units)
    {
        if (units.Length == 2)
        {
            //KM OR km
            if ((units[0] == 'K' || units[0] == 'k') && (units[1] == 'M' || units[1] == 'm'))
            {
                return value / 0.001;
            }
            // FT OR ft
            else if ((units[0] == 'F' || units[0] == 'f') && (units[1] == 'T' || units[1] == 't'))
            {
                return value / 3.28084;
            }
            // MI OR mi
            else if ((units[0] == 'M' || units[0] == 'm') && (units[1] == 'I' || units[1] == 'i'))
            {
                return value / 0.000621371;
            }
        }

        return value;
    }


    /// <summary>
    /// Helper to convert meters to kilometers, feet, or miles
    /// </summary>
    public static double ConvertMetersToUnits(double value, byte[] units)
    {
        if (units.Length == 2)
        {
            //KM OR km
            if ((units[0] == 'K' || units[0] == 'k') && (units[1] == 'M' || units[1] == 'm'))
            {
                return value * 0.001;
            }
            //FT OR ft
            else if ((units[0] == 'F' || units[0] == 'f') && (units[1] == 'T' || units[1] == 't'))
            {
                return value * 3.28084;
            }
            // MI OR mi
            else if ((units[0] == 'M' || units[0] == 'm') && (units[1] == 'I' || units[1] == 'i'))
            {
                return value * 0.000621371;
            }
        }

        return value;
    }
}