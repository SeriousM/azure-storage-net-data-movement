//------------------------------------------------------------------------------
// <copyright file="AzureBlobLocation.cs" company="Microsoft">
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
    internal class AzureBlobLocation : TransferLocation
    {
        private const string BlobName = "Blob";
        private const string AccessConditionName = "AccessCondition";
        private const string CheckedAccessConditionName = "CheckedAccessCondition";
        private const string RequestOptionsName = "RequestOptions";
        private const string ETagName = "ETag";
        private const string BlockIDPrefixName = "BlockIDPrefix";

        [DataMember]
        private SerializableCloudBlob blobSerializer;

        [DataMember]
        private SerializableAccessCondition accessCondition;

        [DataMember]
        private SerializableRequestOptions requestOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobLocation"/> class.
        /// </summary>
        /// <param name="blob">CloudBlob instance as a location in a transfer job. 
        /// It could be a source, a destination.</param>
        public AzureBlobLocation(CloudBlob blob)
        {
            if (null == blob)
            {
                throw new ArgumentNullException("blob");
            }

            this.Blob = blob;
        }

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.AzureBlob;
            }
        }

        /// <summary>
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance
        {
            get
            {
                return this.Blob;
            }
        }

        /// <summary>
        /// Gets or sets access condition for this location.
        /// This property only takes effact when the location is a blob or an azure file.
        /// </summary>
        public AccessCondition AccessCondition 
        {
            get
            {
                return SerializableAccessCondition.GetAccessCondition(this.accessCondition);
            }

            set
            {
                SerializableAccessCondition.SetAccessCondition(ref this.accessCondition, value);
            }
        }

        /// <summary>
        /// Gets blob location in this instance.
        /// </summary>
        public CloudBlob Blob
        {
            get
            {
                return SerializableCloudBlob.GetBlob(this.blobSerializer);
            }

            private set
            {
                SerializableCloudBlob.SetBlob(ref this.blobSerializer, value);
            }
        }

        [DataMember]
        internal string ETag
        {
            get;
            set;
        }

        [DataMember]
        internal bool CheckedAccessCondition
        {
            get;
            set;
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

        [DataMember]
        internal string BlockIdPrefix
        {
            get;
            set;
        }

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            try
            {
                this.Blob.Container.FetchAttributesAsync(null, Transfer_RequestOptions.DefaultBlobRequestOptions, Utils.GenerateOperationContext(null)).Wait();
            }
            catch (AggregateException e)
            {
                StorageException innnerException = e.Flatten().InnerExceptions[0] as StorageException;
                
                // If doesn't have permission to access the container, it might still have proper permission to acess blobs in the container.   
                // Here swallows the errors that could be possible thrown out when it cannot access the container.  
                // With some older version of SAS token, it reports error of NotFound (404),  
                // with other newer version of SAS token, it reports error of Forbidden (403)  
                // swallows both here.  
                if (this.Blob.Container.ServiceClient.Credentials.IsSharedKey
                    || !Utils.IsExpectedHttpStatusCodes(innnerException, HttpStatusCode.Forbidden, HttpStatusCode.NotFound))
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Update credentials of blob or azure file location.
        /// </summary>
        /// <param name="credentials">Storage credentials to be updated in blob or azure file location.</param>
        public void UpdateCredentials(StorageCredentials credentials)
        {
            this.blobSerializer.UpdateStorageCredentials(credentials);
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.Blob.SnapshotQualifiedUri.ToString();
        }
    }
}
