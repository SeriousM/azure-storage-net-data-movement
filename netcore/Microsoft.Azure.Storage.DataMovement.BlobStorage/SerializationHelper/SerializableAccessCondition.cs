//------------------------------------------------------------------------------
// <copyright file="SerializableAccessCondition.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.SerializationHelper
{
    using System;
    using System.Runtime.Serialization;

    [DataContract]
    internal sealed class SerializableAccessCondition
    {
        private const string IfMatchETagName = "IfMatchETag";
        private const string IfModifiedSinceTimeName = "IfModifiedSinceTime";
        private const string IfNoneMatchETagName = "IfNoneMatchETag";
        private const string IfNotModifiedSinceTimeName = "IfNotModifiedSinceTime";
        private const string IfSequenceNumberEqualName = "IfSequenceNumberEqual";
        private const string IfSequenceNumberLessThanName = "IfSequenceNumberLessThan";
        private const string IfSequenceNumberLessThanOrEqualName = "IfSequenceNumberLessThanOrEqual";
        private const string LeaseIdName = "LeaseId";
        
        private AccessCondition accessCondition;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableAccessCondition" /> class.
        /// </summary>
        public SerializableAccessCondition()
        {
        }

        #region Serialization helpers

        [DataMember]
        private string ifMatchETag;
        
        [DataMember]
        private DateTimeOffset? ifModifiedSinceTime;
        
        [DataMember]
        private string ifNoneMatchETag;
        
        [DataMember]
        private DateTimeOffset? ifNotModifiedSinceTime;
        
        [DataMember]
        private long? ifSequenceNumberEqual;
        
        [DataMember]
        private long? ifSequenceNumberLessThan;
        
        [DataMember]
        private long? ifSequenceNumberLessThanOrEqual;
        
        [DataMember]
        private string leaseId;
        
        /// <summary>
        /// Serializes the object by extracting key data from the underlying AccessCondition
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            ifMatchETag = null == accessCondition ? null : this.accessCondition.IfMatchETag;
            ifModifiedSinceTime = null == accessCondition ? null : this.accessCondition.IfModifiedSinceTime;
            ifNoneMatchETag = null == accessCondition ? null : this.accessCondition.IfNoneMatchETag;
            ifNotModifiedSinceTime = null == accessCondition ? null : this.accessCondition.IfNotModifiedSinceTime;
            ifSequenceNumberEqual = null == accessCondition ? null : this.accessCondition.IfSequenceNumberEqual;
            ifSequenceNumberLessThan = null == accessCondition ? null : this.accessCondition.IfSequenceNumberLessThan;
            ifSequenceNumberLessThanOrEqual = null == accessCondition ? null : this.accessCondition.IfSequenceNumberLessThanOrEqual;
            leaseId = null == accessCondition ? null : this.accessCondition.LeaseId;
        }

        /// <summary>
        /// Initializes a deserialized AccessCondition
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            if (!string.IsNullOrEmpty(ifMatchETag)
                || null != ifModifiedSinceTime
                || !string.IsNullOrEmpty(ifNoneMatchETag)
                || null != ifNotModifiedSinceTime
                || null != ifSequenceNumberEqual
                || null != ifSequenceNumberLessThan
                || null != ifSequenceNumberLessThanOrEqual
                || !string.IsNullOrEmpty(leaseId))
            {
                this.accessCondition = new AccessCondition()
                {
                    IfMatchETag = ifMatchETag,
                    IfModifiedSinceTime = ifModifiedSinceTime,
                    IfNoneMatchETag = ifNoneMatchETag,
                    IfNotModifiedSinceTime = ifNotModifiedSinceTime,
                    IfSequenceNumberEqual = ifSequenceNumberEqual,
                    IfSequenceNumberLessThan = ifSequenceNumberLessThan,
                    IfSequenceNumberLessThanOrEqual = ifSequenceNumberLessThanOrEqual,
                    LeaseId = leaseId
                };
            }
            else
            {
                this.accessCondition = null;
            }
        }
#endregion // Serialization helpers

        internal AccessCondition AccessCondition
        {
            get
            {
                return this.accessCondition;
            }

            set
            {
                this.accessCondition = value;
            }
        }

        internal static AccessCondition GetAccessCondition(SerializableAccessCondition serialization)
        {
            if (null == serialization)
            {
                return null;
            }

            return serialization.AccessCondition;
        }

        internal static void SetAccessCondition(
            ref SerializableAccessCondition serialization, 
            AccessCondition value)
        {            
            if ((null == serialization)
                && (null == value))
            {
                return;
            }

            if (null != serialization)
            {
                serialization.AccessCondition = value;
            }
            else
            {
                serialization = new SerializableAccessCondition()
                {
                    AccessCondition = value
                };
            }
        }
    }
}
