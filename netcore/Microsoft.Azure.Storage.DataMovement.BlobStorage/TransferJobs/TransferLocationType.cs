//------------------------------------------------------------------------------
// <copyright file="TransferLocationType.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    internal enum TransferLocationType
    {
        FilePath,
        Stream,
        AzureBlob,
        SourceUri,
        LocalDirectory,
        AzureBlobDirectory,
    }
}
