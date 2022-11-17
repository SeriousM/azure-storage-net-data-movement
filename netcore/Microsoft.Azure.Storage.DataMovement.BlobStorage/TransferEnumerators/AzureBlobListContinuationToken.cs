//------------------------------------------------------------------------------
// <copyright file="AzureBlobListContinuationToken.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Runtime.Serialization;
    using Microsoft.Azure.Storage.Blob;

    [DataContract]
    internal sealed class AzureBlobListContinuationToken : ListContinuationToken
    {
        private const string BlobContinuationTokenName = "BlobContinuationToken";
        private const string BlobNameName = "BlobName";
        private const string HasSnapshotName = "HasSnapshot";
        private const string SnapshotTimeName = "SnapshotTime";

        public AzureBlobListContinuationToken(BlobContinuationToken blobContinuationToken, string blobName, DateTimeOffset? snapshotTime)
        {
            this.BlobContinuationToken = blobContinuationToken;
            this.BlobName = blobName;
            this.SnapshotTime = snapshotTime;
        }

        [DataMember]
        public BlobContinuationToken BlobContinuationToken
        {
            get;
            private set;
        }

        [DataMember]
        public string BlobName
        {
            get;
            private set;
        }

        [DataMember]
        public DateTimeOffset? SnapshotTime
        {
            get;
            private set;
        }

    }
}
