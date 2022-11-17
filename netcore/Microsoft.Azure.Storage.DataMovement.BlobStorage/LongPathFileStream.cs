//------------------------------------------------------------------------------
// <copyright file="LongPathFileStream.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Diagnostics;

    internal static class LongPath
    {
        private const string ExtendedPathPrefix = @"\\?\";
        private const string UncPathPrefix = @"\\";
        private const string UncExtendedPrefixToInsert = @"?\UNC\";
        // \\?\, \\.\, \??\
        internal const int DevicePrefixLength = 4;

        /// <summary>
        /// Returns true if the path specified is relative to the current drive or working directory.
        /// Returns false if the path is fixed to a specific drive or UNC path.  This method does no
        /// validation of the path (URIs will be returned as relative as a result).
        /// </summary>
        internal static bool IsPartiallyQualified(string path)
        {
            if (path.Length < 2)
            {
                // It isn't fixed, it must be relative.  There is no way to specify a fixed
                // path with one character (or less).
                return true;
            }

            if (IsDirectorySeparator(path[0]))
            {
                // There is no valid way to specify a relative path with two initial slashes or
                // \? as ? isn't valid for drive relative paths and \??\ is equivalent to \\?\
                return !(path[1] == '?' || IsDirectorySeparator(path[1]));
            }

            // The only way to specify a fixed path that doesn't begin with two slashes
            // is the drive, colon, slash format- i.e. C:\
            return !((path.Length >= 3)
                && (path[1] == Path.VolumeSeparatorChar)
                && IsDirectorySeparator(path[2])
                // To match old behavior we'll check the drive character for validity as the path is technically
                // not qualified if you don't have a valid drive. "=:\" is the "=" file's default data stream.
                && IsValidDriveChar(path[0]));
        }

        /// <summary>
        /// Returns true if the given character is a valid drive letter
        /// </summary>
        internal static bool IsValidDriveChar(char value)
        {
            return ((value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z'));
        }

        /// <summary>
        /// Returns true if the path uses any of the DOS device path syntaxes. ("\\.\", "\\?\", or "\??\")
        /// </summary>
        internal static bool IsDevice(string path)
        {
            // If the path begins with any two separators is will be recognized and normalized and prepped with
            // "\??\" for internal usage correctly. "\??\" is recognized and handled, "/??/" is not.
            return IsExtended(path)
                ||
                (
                    path.Length >= DevicePrefixLength
                    && IsDirectorySeparator(path[0])
                    && IsDirectorySeparator(path[1])
                    && (path[2] == '.' || path[2] == '?')
                    && IsDirectorySeparator(path[3])
                );
        }

        /// <summary>
        /// Returns true if the path uses the canonical form of extended syntax ("\\?\" or "\??\"). If the
        /// path matches exactly (cannot use alternate directory separators) Windows will skip normalization
        /// and path length checks.
        /// </summary>
        internal static bool IsExtended(string path)
        {
            // While paths like "//?/C:/" will work, they're treated the same as "\\.\" paths.
            // Skipping of normalization will *only* occur if back slashes ('\') are used.
            return path.Length >= DevicePrefixLength
                && path[0] == '\\'
                && (path[1] == '\\' || path[1] == '?')
                && path[2] == '?'
                && path[3] == '\\';
        }

        private static bool IsDirectorySeparator(char ch)
        {
            return ch == Path.DirectorySeparatorChar || ch == Path.AltDirectorySeparatorChar;
        }

        public static string ToUncPath(string path)
        {
            if (IsDevice(path))
            {
                return LongPath.GetFullPath(path);
            }

            if (IsPartiallyQualified(path))
            {
                path = LongPath.GetFullPath(path);
                if (IsDevice(path))
                    return path;
                else
                    return ExtendedPathPrefix + path;
            }

            //// Given \\server\share in longpath becomes \\?\UNC\server\share
            if (path.StartsWith(UncPathPrefix, StringComparison.OrdinalIgnoreCase))
                return LongPath.GetFullPath(path.Insert(2, UncExtendedPrefixToInsert));

            return LongPath.GetFullPath(ExtendedPathPrefix + path);
        }

        public static string GetFullPath(string path)
        {
            return Path.GetFullPath(path);
        }

        public static string Combine(string path1, string path2)
        {
            return Path.Combine(path1, path2);
        }

        /// <summary>
        /// Returns the directory information for the specified path string.
        /// </summary>
        /// <param name="path">The path of a file or directory.</param>
        /// <returns></returns>
        public static string GetDirectoryName(string path)
        {
            return Path.GetDirectoryName(path);
        }

        private static int GetRootLength(string path)
        {
            int pathLength = path.Length;
            int i = 0;
            int volumeSeparatorLength = 2;  // Length to the colon "C:"
            int uncRootLength = 2;          // Length to the start of the server name "\\"

            const string ExtendedPathPrefix = @"\\?\";
            const string UncExtendedPathPrefix = @"\\?\UNC\";
            bool extendedSyntax = path.StartsWith(ExtendedPathPrefix, StringComparison.Ordinal);
            bool extendedUncSyntax = path.StartsWith(UncExtendedPathPrefix, StringComparison.Ordinal);
            if (extendedSyntax)
            {
                // Shift the position we look for the root from to account for the extended prefix
                if (extendedUncSyntax)
                {
                    // "\\" -> "\\?\UNC\"
                    uncRootLength = UncExtendedPathPrefix.Length;
                }
                else
                {
                    // "C:" -> "\\?\C:"
                    volumeSeparatorLength += ExtendedPathPrefix.Length;
                }
            }

            if ((!extendedSyntax || extendedUncSyntax) && pathLength > 0 && IsDirectorySeparator(path[0]))
            {
                // UNC or simple rooted path (e.g. "\foo", NOT "\\?\C:\foo")

                i = 1; //  Drive rooted (\foo) is one character
                if (extendedUncSyntax || (pathLength > 1 && IsDirectorySeparator(path[1])))
                {
                    // UNC (\\?\UNC\ or \\), scan past the next two directory separators at most
                    // (e.g. to \\?\UNC\Server\Share or \\Server\Share\)
                    i = uncRootLength;
                    int n = 2; // Maximum separators to skip
                    while (i < pathLength && (!IsDirectorySeparator(path[i]) || --n > 0)) i++;
                }
            }
            else if (pathLength >= volumeSeparatorLength && path[volumeSeparatorLength - 1] == Path.VolumeSeparatorChar)
            {
                // Path is at least longer than where we expect a colon, and has a colon (\\?\A:, A:)
                // If the colon is followed by a directory separator, move past it
                i = volumeSeparatorLength;
                if (pathLength >= volumeSeparatorLength + 1 && IsDirectorySeparator(path[volumeSeparatorLength])) i++;
            }
            return i;
        }

        public static string GetFileNameWithoutExtension(string path)
        {

            return Path.GetFileNameWithoutExtension(path);
        }

        public static string GetFileName(string path)
        {
            return Path.GetFileName(path);
        }

        private static int FindFileNameIndex(string path)
        {
            Debug.Assert(path != null);

            for (int i = path.Length - 1; i >= 0; --i)
            {
                char ch = path[i];
                if (ch == Path.DirectorySeparatorChar || ch == Path.AltDirectorySeparatorChar || ch == Path.VolumeSeparatorChar)
                    return i + 1;
            }

            return 0;
        }
    }

    internal static class LongPathDirectory
    {
        public static bool Exists(string path)
        {
            return Directory.Exists(path);
        }

        public static string[] GetFiles(string path)
        {
            return Directory.GetFiles(path);
        }

        /// <summary>
        /// Creates all directories and subdirectories in the specified path unless they already exist.
        /// </summary>
        /// <param name="path">The directory to create.</param>
        public static void CreateDirectory(string path)
        {
            Directory.CreateDirectory(path);
        }

        public enum FilesOrDirectory
        {
            None,
            File,
            Directory,
            All
        };

        public static IEnumerable<string> EnumerateFileSystemEntries(string path, string searchPattern, SearchOption searchOption)
        {
#if (DEBUG && TEST_HOOK)
            FaultInjectionPoint fip = new FaultInjectionPoint(FaultInjectionPoint.FIP_ThrowExceptionOnDirectory);
            string fiValue;

            if (fip.TryGetValue(out fiValue) && !String.IsNullOrEmpty(fiValue))
            {
                if (!fiValue.EndsWith(Path.DirectorySeparatorChar.ToString()))
                {
                    fiValue = fiValue + Path.DirectorySeparatorChar.ToString();
                }

                if (path.EndsWith(fiValue, StringComparison.OrdinalIgnoreCase))
                {
                    throw new Exception("test exception thrown because of FIP_ThrowExceptionOnDirectory is enabled");
                }
            }
#endif
            return Directory.EnumerateFileSystemEntries(path, searchPattern, searchOption);
        }

        public static IEnumerable<string> EnumerateFileSystemEntries(string path, string searchPattern, SearchOption searchOption, FilesOrDirectory filter)
        {
            return Directory.EnumerateFileSystemEntries(path, searchPattern, searchOption);
        }
    }

    internal static partial class LongPathFile
    {
        public static FileStream Open(string filePath, FileMode mode, FileAccess access, FileShare share)
        {
            return new FileStream(filePath, mode, access, share);
        }

        public static void SetAttributes(string path, FileAttributes fileAttributes)
        {
#if DEBUG
            if (null != TestHookCallbacks.SetFileAttributesCallback)
            {
                TestHookCallbacks.SetFileAttributesCallback(path, fileAttributes);
                return;
            }
#endif

            File.SetAttributes(path, fileAttributes);
        }

        public static FileAttributes GetAttributes(string path)
        {
            return File.GetAttributes(path);
        }

        public static void GetFileProperties(string path, out DateTimeOffset? creationTime, out DateTimeOffset? lastWriteTime, out FileAttributes? fileAttributes
            , bool isDirectory = false
            )
        {
            fileAttributes = File.GetAttributes(path);

            if (isDirectory)
            {
                creationTime = Directory.GetCreationTimeUtc(path);
                lastWriteTime = Directory.GetLastWriteTimeUtc(path);
            }
            else
            {
                creationTime = File.GetCreationTimeUtc(path);
                lastWriteTime = File.GetLastWriteTimeUtc(path);
            }

#if DEBUG
            if (null != TestHookCallbacks.GetFileAttributesCallback)
            {
                fileAttributes = TestHookCallbacks.GetFileAttributesCallback(path);
            }
#endif
        }

        public static void SetFileTime(string path, DateTimeOffset creationTimeUtc, DateTimeOffset lastWriteTimeUtc, bool isDirectory = false)
        {
            if (isDirectory)
            {
                Directory.SetCreationTimeUtc(path, creationTimeUtc.UtcDateTime);
                Directory.SetLastWriteTimeUtc(path, lastWriteTimeUtc.UtcDateTime);
            }
            else
            {
                File.SetCreationTimeUtc(path, creationTimeUtc.UtcDateTime);
                File.SetLastWriteTimeUtc(path, lastWriteTimeUtc.UtcDateTime);
            }
        }
    }
}
