//------------------------------------------------------------------------------
// <copyright file="UploadDirectoryOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement.Interop;

    /// <summary>
    /// Represents a set of options that may be specified for upload directory operation
    /// </summary>
    public sealed class UploadDirectoryOptions : DirectoryOptions
    {
        private bool followSymlink = false;

        /// <summary>
        /// Gets or sets type of destination blob. This option takes effect only when uploading to Azure blob storage.
        /// If blob type is not specified, BlockBlob is used.
        /// </summary>
        public BlobType BlobType { get; set; }

        /// <summary>
        /// Gets or sets whether to follow symlinked directories. This option only works in Unix/Linux platforms.
        /// </summary>
        public bool FollowSymlink
        {
            get
            {
                return this.followSymlink;
            }

            set
            {
                if (value && !CrossPlatformHelpers.IsLinux)
                {
                    throw new PlatformNotSupportedException();
                }

                this.followSymlink = value;
            }
        }

        /// <summary>
        /// Gets or sets a value which specifies the name of the encryption scope to use to encrypt the data provided in the request.
        /// This value only takes effect when destination is Azure Blob Service.
        /// </summary>
        public string EncryptionScope { get; set; }
    }
}
