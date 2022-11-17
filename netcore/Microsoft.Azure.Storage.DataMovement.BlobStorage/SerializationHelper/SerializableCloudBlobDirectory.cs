//------------------------------------------------------------------------------
// <copyright file="SerializableCloudBlobDirectory.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Globalization;
    using System.Runtime.Serialization;
    using Microsoft.Azure.Storage.Auth;
    using Microsoft.Azure.Storage.Blob;

    /// <summary>
    /// A utility class for serializing and de-serializing <see cref="CloudBlobDirectory"/> object.
    /// </summary>
    [DataContract]
    internal class SerializableCloudBlobDirectory
    {
        /// <summary>
        /// Serialization field name for cloud blob container uri.
        /// </summary>
        private const string ContainerUriName = "ContainerUri";

        /// <summary>
        /// Serialization field name for cloud blob directory prefix.
        /// </summary>
        private const string RelativeAddressName = "RelativeAddress";

        /// <summary>
        /// Cloud blob container uri for the blob directory.
        /// </summary>
        [DataMember]
        private Uri containerUri;

        /// <summary>
        /// Prefix of the cloud blob directory.
        /// </summary>
        [DataMember]
        private string relativeAddress;

        /// <summary>
        /// Stores the <see cref="CloudBlobDirectory"/> object,
        /// </summary>
        private CloudBlobDirectory blobDir;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableCloudBlobDirectory" /> class.
        /// </summary>
        /// <param name="blobDir">A <see cref="CloudBlobDirectory" /> object. </param>
        public SerializableCloudBlobDirectory(CloudBlobDirectory blobDir)
        {
            this.blobDir = blobDir;
            this.containerUri = this.blobDir.Container.Uri;
            this.relativeAddress = this.blobDir.Prefix;
        }

        /// <summary>
        /// Initializes an instance of SerializableCloudBlobDirectory after
        /// deserialization via DCS
        /// </summary>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            this.CreateCloudBlobDirectoryInstance(null);
        }

        /// <summary>
        /// Gets the target <see cref="CloudBlobDirectory"/> object.
        /// </summary>
        internal CloudBlobDirectory BlobDirectory
        {
            get
            {
                return this.blobDir;
            }
        }

        /// <summary>
        /// Updates the account credentials associated with the target <see cref="CloudBlobDirectory"/> object.
        /// </summary>
        /// <param name="credentials">A <see cref="StorageCredentials"/> object.</param>
        internal void UpdateStorageCredentials(StorageCredentials credentials)
        {
            if (null != this.blobDir && this.blobDir.ServiceClient.Credentials == credentials)
            {
                return;
            }

            this.CreateCloudBlobDirectoryInstance(credentials);
        }

        /// <summary>
        /// Creates a <see cref="CloudBlobDirectory"/> object using the specified account credentials.
        /// </summary>
        /// <param name="credentials">A <see cref="StorageCredentials"/> object.</param>
        private void CreateCloudBlobDirectoryInstance(StorageCredentials credentials)
        {
            if (null == this.containerUri)
            {
                throw new InvalidOperationException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        Resources.ParameterCannotBeNullException,
                        "containerUri"));
            }

            CloudBlobContainer container = new CloudBlobContainer(this.containerUri, credentials);
            this.blobDir = container.GetDirectoryReference(this.relativeAddress);
        }
    }
}
