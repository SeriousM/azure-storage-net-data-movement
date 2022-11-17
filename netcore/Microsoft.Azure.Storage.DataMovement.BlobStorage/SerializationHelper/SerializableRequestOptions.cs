//------------------------------------------------------------------------------
// <copyright file="SerializableRequestOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.SerializationHelper
{
  using System.Diagnostics;
    using System.Runtime.Serialization;
    using Microsoft.Azure.Storage.Blob;

    [DataContract]
    internal abstract class SerializableRequestOptions
    {
        protected SerializableRequestOptions()
        { 
        }

        protected abstract IRequestOptions RequestOptions
        {
            get;
            set;
        }

        internal static IRequestOptions GetRequestOptions(SerializableRequestOptions serializer)
        {
            if (null == serializer)
            {
                return null;
            }

            return serializer.RequestOptions;
        }

        internal static void SetRequestOptions(ref SerializableRequestOptions serializer, IRequestOptions requestOptions)
        {
            if (null == serializer && null == requestOptions)
            {
                return;
            }

            if (null == serializer)
            {
                serializer = CreateSerializableRequestOptions(requestOptions);
            }
            else
            {
                serializer.RequestOptions = requestOptions;
            }
        }

        private static SerializableRequestOptions CreateSerializableRequestOptions(IRequestOptions requestOptions)
        {
            Debug.Assert(requestOptions is BlobRequestOptions, "Request options should be an instance of BlobRequestOptions when code reach here.");

            return new SerializableBlobRequestOptions()
            {
              RequestOptions = requestOptions
            };
        }
    }
}
