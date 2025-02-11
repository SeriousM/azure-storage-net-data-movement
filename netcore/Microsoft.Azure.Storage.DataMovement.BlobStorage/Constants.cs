//------------------------------------------------------------------------------
// <copyright file="Constants.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Reflection;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Constants for use with the transfer classes.
    /// </summary>
    public static class Constants
    {
        /// <summary>
        /// Stores the max block size, 100MB.
        /// </summary>
        public const int MaxBlockSize = 100 * 1024 * 1024;

        /// <summary>
        /// Default block size when transferring block blob, 8MB.
        /// </summary>
        public const int DefaultBlockBlobBlockSize = 8 * 1024 * 1024;

        /// <summary>
        /// Default size for a chunk transferring, 4MB.
        /// </summary>
        public const int DefaultTransferChunkSize = 4 * 1024 * 1024;

        /// <summary>
        /// Default memory chunk size of memory pool, 4MB.
        /// </summary>
        public const int DefaultMemoryChunkSize = 4 * 1024 * 1024;

        /// <summary>
        /// The metadata key for empty blobs which represent directories in file system.
        /// </summary>
        public const string DirectoryBlobMetadataKey = "hdi_isfolder";

        /// <summary>
        /// A file relative path can be at most 1024 character long based on Windows Azure documentation.
        /// </summary>
        internal const int MaxRelativePathLength = 1024;

        //TODO: How to tune this???

        /// <summary>
        /// Minimum block size, 4MB.
        /// </summary>
        internal const int MinBlockSize = 4 * 1024 * 1024;

        /// <summary>
        /// Stores the max append blob file size, 50000 * 4M.
        /// </summary>
        internal const long MaxAppendBlobFileSize = (long)50000 * 4 * 1024 * 1024;

        /// <summary>
        /// Stores the max block blob file size, 50000 * 100M.
        /// </summary>
        internal const long MaxBlockBlobFileSize = (long)50000 * 100 * 1024 * 1024;

        /// <summary>
        /// Max transfer window size. 
        /// There can be multiple threads to transfer a file, 
        /// and we need to record transfer window 
        /// and have constant length for a transfer entry record in restart journal,
        /// so set a limitation for transfer window here.
        /// </summary>
        internal const int MaxCountInTransferWindow = 128;

        /// <summary>
        /// Length to get page ranges in one request. 
        /// In blog <c>http://blogs.msdn.com/b/windowsazurestorage/archive/2012/03/26/getting-the-page-ranges-of-a-large-page-blob-in-segments.aspx</c>,
        /// it says that it's safe to get page ranges of 150M in one request.
        /// We use 148MB which is multiples of 4MB.
        /// </summary>
        internal const long PageRangesSpanSize = 148 * 1024 * 1024;

        /// <summary>
        /// Percentage of available we'll try to use for our memory cache.
        /// </summary>
        internal const double MemoryCacheMultiplier = 0.5;

        /// <summary>
        /// Maximum amount of memory to use for our memory cache.
        /// </summary>
        internal static readonly long MemoryCacheMaximum = GetMemoryCacheMaximum();

        /// <summary>
        /// Maximum amount of cells in memory manager.
        /// </summary>
        internal const int MemoryManagerCellsMaximum = 8 * 1024;

        /// <summary>
        /// The life time in minutes of SAS auto generated for asynchronous copy.
        /// </summary>
        internal const int CopySASLifeTimeInMinutes = 7 * 24 * 60;

        /// <summary>
        /// The time in milliseconds to wait to refresh copy status for asynchronous copy.
        /// In order to avoid refreshing the status too aggressively for large copy job and
        /// meanwhile provide a timely update for small copy job, the wait time increases
        /// from 0.1 second to 5 seconds gradually and remains 5 seconds afterwards.
        /// </summary>
        internal const long CopyStatusRefreshMinWaitTimeInMilliseconds = 100;

        internal const long CopyStatusRefreshMaxWaitTimeInMilliseconds = 5 * 1000;

        internal const long CopyStatusRefreshWaitTimeMaxRequestCount = 100;

        /// <summary>
        /// Asynchronous copy decreases status refresh wait time to 0.1s if there's less than 500 MB
        /// data to copy in order to detect the job completion in time.
        /// </summary>
        internal const long CopyApproachingFinishThresholdInBytes = 500 * 1024 * 1024;

        /// <summary>
        /// Multiplier to calculate number of entries listed in one segment.
        /// Formula is: Concurrency * <c>ListSegmentLengthMultiplier</c>.
        /// </summary>
        internal const int ListSegmentLengthMultiplier = 8;

        internal const string BlobTypeMismatch = "Blob type of the blob reference doesn't match blob type of the blob.";

        /// <summary>
        /// The maximum size of a block blob that can be uploaded with a single Put Blob request.
        /// </summary>
        internal const long MaxSinglePutBlobSize = 256 * 1024 * 1024;

        /// <summary>
        /// The product name used in UserAgent header.
        /// </summary>
        internal const string UserAgentProductName = "DataMovement";

        /// <summary>
        /// UserAgent header.
        /// </summary>
        internal static readonly string UserAgent = GetUserAgent();

        internal static readonly string FormatVersion = GetFormatVersion();

        /// <summary>
        /// Gets the UserAgent string.
        /// </summary>
        /// <returns>UserAgent string.</returns>
        private static string GetUserAgent()
        {
            AssemblyName assemblyName = typeof(Constants).GetTypeInfo().Assembly.GetName();

            return UserAgentProductName + "/" + assemblyName.Version.ToString();
        }

        private static string GetFormatVersion()
        {
            AssemblyName assemblyName = typeof(Constants).GetTypeInfo().Assembly.GetName();

            return assemblyName.Name + "/" + assemblyName.Version.ToString();
        }

        private static long GetMemoryCacheMaximum()
        {
            return 8 == Marshal.SizeOf(new IntPtr()) ? (long)2 * 1024 * 1024 * 1024 : (long)512 * 1024 * 1024;
        }

        internal static readonly TimeSpan DefaultEnumerationWaitTimeOut = TimeSpan.FromSeconds(10);
    }
}
