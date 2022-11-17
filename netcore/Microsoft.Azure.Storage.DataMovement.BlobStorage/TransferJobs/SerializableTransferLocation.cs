//------------------------------------------------------------------------------
// <copyright file="SerializableTransferLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
  using System.Runtime.Serialization;

    [DataContract]
    [KnownType(typeof(AzureBlobDirectoryLocation))]
    [KnownType(typeof(AzureBlobLocation))]
    [KnownType(typeof(DirectoryLocation))]
    [KnownType(typeof(FileLocation))]
    // StreamLocation intentionally omitted because it is not serializable
    [KnownType(typeof(UriLocation))]
    internal sealed class SerializableTransferLocation
    {
        private const string TransferLocationTypeName = "LocationType";
        private const string TransferLocationName = "Location";

        public SerializableTransferLocation(TransferLocation location)
        {
            this.Location = location;
        }

        [DataMember]
        public TransferLocation Location
        {
            get;
            private set;
        }
    }
}
