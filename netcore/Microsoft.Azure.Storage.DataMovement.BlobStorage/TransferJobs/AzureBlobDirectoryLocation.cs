//------------------------------------------------------------------------------
// <copyright file="AzureBlobDirectoryLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Net;
    using System.Runtime.Serialization;
    using Microsoft.Azure.Storage.Auth;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement.SerializationHelper;

    [DataContract]
    [KnownType(typeof(SerializableBlobRequestOptions))]
    internal class AzureBlobDirectoryLocation : TransferLocation
    {
        private const string BlobDirName = "CloudBlobDir";
        private const string RequestOptionsName = "RequestOptions";

        [DataMember]
        private SerializableCloudBlobDirectory blobDirectorySerializer;

        [DataMember]
        private SerializableRequestOptions requestOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobDirectoryLocation"/> class.
        /// </summary>
        /// <param name="blobDir">CloudBlobDirectory instance as a location in a transfer job.
        /// It could be a source, a destination.</param>
        public AzureBlobDirectoryLocation(CloudBlobDirectory blobDir)
        {
            if (null == blobDir)
            {
                throw new ArgumentNullException("blobDir");
            }

            this.blobDirectorySerializer = new SerializableCloudBlobDirectory(blobDir);
        }

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.AzureBlobDirectory;
            }
        }

        /// <summary>
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance
        {
            get
            {
                return this.BlobDirectory;
            }
        }

        /// <summary>
        /// Gets Azure blob directory location in this instance.
        /// </summary>
        public CloudBlobDirectory BlobDirectory
        {
            get
            {
                return this.blobDirectorySerializer.BlobDirectory;
            }
        }

        /// <summary>
        /// Gets or sets BlobRequestOptions when send request to this location.
        /// </summary>
        internal BlobRequestOptions BlobRequestOptions
        {
            get
            {
                return (BlobRequestOptions)SerializableBlobRequestOptions.GetRequestOptions(this.requestOptions);
            }
            set
            {
                SerializableRequestOptions.SetRequestOptions(ref this.requestOptions, value);
            }
        }

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            try
            {
                this.BlobDirectory.Container.FetchAttributesAsync(null, Transfer_RequestOptions.DefaultBlobRequestOptions, Utils.GenerateOperationContext(null)).Wait();
            }
            catch(AggregateException e)
            {
                StorageException innnerException = e.Flatten().InnerExceptions[0] as StorageException;

                // If doesn't have permission to access the container, it might still have proper permission to acess blobs in the container.   
                // Here swallows the errors that could be possible thrown out when it cannot access the container.  
                // With some older version of SAS token, it reports error of NotFound (404),  
                // with other newer version of SAS token, it reports error of Forbidden (403)  
                // swallows both here.  
                if (this.BlobDirectory.Container.ServiceClient.Credentials.IsSharedKey
                    || !Utils.IsExpectedHttpStatusCodes(innnerException, HttpStatusCode.Forbidden, HttpStatusCode.NotFound))
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Update credentials of azure blob directory location.
        /// </summary>
        /// <param name="credentials">New storage credentials to use.</param>
        public void UpdateCredentials(StorageCredentials credentials)
        {
            this.blobDirectorySerializer.UpdateStorageCredentials(credentials);
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.BlobDirectory.Uri.ToString();
        }
    }
}
