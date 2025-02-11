//------------------------------------------------------------------------------
// <copyright file="CopyOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    /// <summary>
    /// Represents a set of options that may be specified for copy operation
    /// </summary>
    public sealed class CopyOptions
    {
        /// <summary>
        /// Gets or sets an <see cref="AccessCondition"/> object that represents the access conditions for the source object. If <c>null</c>, no condition is used.
        /// </summary>
        public AccessCondition SourceAccessCondition { get; set; }

        /// <summary>
        /// Gets or sets an <see cref="AccessCondition"/> object that represents the access conditions for the destination object. If <c>null</c>, no condition is used.
        /// </summary>
        public AccessCondition DestinationAccessCondition { get; set; }

        /// <summary>
        /// Gets or sets a value which specifies the name of the encryption scope to use to encrypt the data provided in the request.
        /// This value only takes effect when destination is Azure Blob Service.
        /// </summary>
        public string EncryptionScope { get; set; }
    }
}
