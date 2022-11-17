//------------------------------------------------------------------------------
// <copyright file="SerializableListContinuationToken.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
  using System.Runtime.Serialization;

    [DataContract]
    [KnownType(typeof(AzureBlobListContinuationToken))]
    [KnownType(typeof(FileListContinuationToken))]
    internal sealed class SerializableListContinuationToken : JournalItem
    {
        private const string ListContinuationTokenTypeName = "TokenType";
        private const string ListContinuationTokenName = "Token";
        private const string TokenTypeFile = "FileListContinuationToken";
        private const string TokenTypeAzureBlob = "AzureBlobListContinuationToken";
        private const string TokenTypeAzureFile = "AzureFileListContinuationToken";

        public SerializableListContinuationToken(ListContinuationToken listContinuationToken)
        {
            this.ListContinuationToken = listContinuationToken;
        }

        [DataMember]
        public ListContinuationToken ListContinuationToken
        {
            get;
            private set;
        }
    }
}
