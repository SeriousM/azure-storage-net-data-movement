//------------------------------------------------------------------------------
// <copyright file="DirectoryLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;

    [DataContract]
    internal class DirectoryLocation : TransferLocation
    {
        private const string DirPathName = "DirPath";

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryLocation"/> class.
        /// </summary>
        /// <param name="dirPath">Path to the local directory as a source/destination to be read from/written to in a transfer.</param>
        public DirectoryLocation(string dirPath)
        {
            if (null == dirPath)
            {
                throw new ArgumentNullException("dirPath");
            }

            if (string.IsNullOrWhiteSpace(dirPath))
            {
                throw new ArgumentException("Directory path should not be an empty string", "dirPath");
            }

            // Normalize directory path to end with back slash.
            if (!dirPath.EndsWith(Path.DirectorySeparatorChar.ToString(), StringComparison.OrdinalIgnoreCase))
            {
                dirPath += Path.DirectorySeparatorChar;
            }

            this.DirectoryPath = dirPath;
        }

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.LocalDirectory;
            }
        }

        /// <summary>
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance
        {
            get
            {
                return this.DirectoryPath;
            }
        }

        /// <summary>
        /// Gets path to the local directory location.
        /// </summary>
        [DataMember]
        public string DirectoryPath
        {
            get;
            private set;
        }

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            DirectoryInfo di = new DirectoryInfo(this.DirectoryPath);
            di.Create();
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.DirectoryPath;
        }
    }
}
