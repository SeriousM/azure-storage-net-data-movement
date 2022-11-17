//------------------------------------------------------------------------------
// <copyright file="FileLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;

    [DataContract]
    internal class FileLocation : TransferLocation
    {
        private const string FilePathName = "FilePath";
        private const string FilePathType = "FilePathType";
        /// <summary>
        /// Initializes a new instance of the <see cref="FileLocation"/> class.
        /// </summary>
        /// <param name="filePath">Path to the local file as a source/destination to be read from/written to in a transfer.</param>
        public FileLocation(string filePath)
        {
            if (null == filePath)
            {
                throw new ArgumentNullException("filePath");
            }

            if (string.IsNullOrWhiteSpace(filePath))
            {
                throw new ArgumentException("message, should not be an empty string", "filePath");
            }

            this.FilePath = filePath;
            this.RelativePath = null;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileLocation"/> class.
        /// </summary>
        /// <param name="filePath">Path to the local file as a source/destination to be read from/written to in a transfer.</param>
        /// <param name="relativePath">Relative path to the local file as a source/destination to be read from/written to in a transfer.</param>
        public FileLocation(string filePath, string relativePath)
        {
            if (null == filePath)
            {
                throw new ArgumentNullException("filePath");
            }

            if (string.IsNullOrWhiteSpace(filePath))
            {
                throw new ArgumentException("message, should not be an empty string", "filePath");
            }

            if (null == relativePath)
            {
                throw new ArgumentNullException("relativePath");
            }

            if (string.IsNullOrWhiteSpace(relativePath))
            {
                throw new ArgumentException("message, should not be an empty string", "relativePath");
            }

            this.FilePath = filePath;
            this.RelativePath = relativePath;
        }


        public void SetDirectoryPath(string directoryPath)
        {
            if(directoryPath != null)
                this.FilePath = LongPath.Combine(directoryPath, this.RelativePath);
        }

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.FilePath;
            }
        }

        /// <summary>
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance
        {
            get
            {
                return this.FilePath;
            }
        }

        /// <summary>
        /// Gets relative path to the local file location.
        /// </summary>
        [DataMember]
        public string RelativePath
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets path to the local file location.
        /// </summary>
        public string FilePath
        {
            get;
            private set;
        }

        #region Serialization helpers
        [DataMember] private string fullPath;

        /// <summary>
        /// Gets or sets a variable to indicate whether the file location will be saved to a streamed journal.
        /// </summary>
        [DataMember]
        public bool IsStreamJournal { get; set; }

        /// <summary>
        /// Serializes the object by extracting FilePath for single object transfer.
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            if(IsStreamJournal == false)
            {
                fullPath = FilePath;
            }
            else
            {
                fullPath = String.IsNullOrEmpty(RelativePath) ? FilePath : null;
            }
        }

        /// <summary>
        /// Initializes a deserialized FilePath
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            FilePath = fullPath;
        }
        #endregion // Serialization helpers

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1804:RemoveUnusedLocals", MessageId = "time")]
        public override void Validate()
        {
            DirectoryInfo di = new DirectoryInfo(Path.GetDirectoryName(this.FilePath));
            DateTime time = di.LastAccessTime;
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.FilePath;
        }
    }
}
