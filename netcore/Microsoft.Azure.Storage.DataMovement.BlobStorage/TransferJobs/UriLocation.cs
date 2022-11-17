//------------------------------------------------------------------------------
// <copyright file="UriLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Runtime.Serialization;

    [DataContract]
    internal class UriLocation : TransferLocation
    {
        private const string UriName = "Uri";

        /// <summary>
        /// Initializes a new instance of the <see cref="UriLocation"/> class.
        /// </summary>
        /// <param name="uri">Uri to the source in an asynchronously copying job.</param>
        public UriLocation(Uri uri)
        {
            if (null == uri)
            {
                throw new ArgumentNullException("uri");
            }

            this.Uri = uri;
        }

        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public override TransferLocationType Type
        {
            get
            {
                return TransferLocationType.SourceUri;
            }
        }

        /// <summary>
        /// Get source/destination instance in transfer.
        /// </summary>
        public override object Instance
        {
            get
            {
                return this.Uri;
            }
        }

        /// <summary>
        /// Gets Uri to the location.
        /// </summary>
        [DataMember]
        public Uri Uri
        {
            get;
            private set;
        }

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public override void Validate()
        {
            return;
        }

        //
        // Summary:
        //     Returns a string that represents the transfer location.
        //
        // Returns:
        //     A string that represents the transfer location.
        public override string ToString()
        {
            return this.Uri.ToString();
        }
    }
}
