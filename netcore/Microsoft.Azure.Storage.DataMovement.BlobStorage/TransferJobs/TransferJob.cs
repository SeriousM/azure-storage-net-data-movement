//------------------------------------------------------------------------------
// <copyright file="TransferJob.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
  using System.Runtime.Serialization;
    using System.Threading;

    /// <summary>
    /// Represents transfer of a single file/blob.
    /// </summary>
    [DataContract]
    [KnownType(typeof(AzureBlobDirectoryLocation))]
    [KnownType(typeof(AzureBlobLocation))]
    [KnownType(typeof(DirectoryLocation))]
    [KnownType(typeof(FileLocation))]
    // StreamLocation intentionally omitted because it is not serializable
    [KnownType(typeof(UriLocation))]
    internal class TransferJob
    {
        private const string SourceName = "Source";
        private const string DestName = "Dest";
        private const string CheckedOverwriteName = "CheckedOverwrite";
        private const string OverwriteName = "Overwrite";
        private const string CopyIdName = "CopyId";
        private const string CheckpointName = "Checkpoint";
        private const string StatusName = "Status";
        
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferJob"/> class.
        /// </summary>
        /// <param name="transfer">Transfer object.</param>
        public TransferJob(SingleObjectTransfer transfer)
        {
            this.Transfer = transfer;

            this.CheckPoint = new SingleObjectCheckpoint();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferJob"/> class.
        /// </summary>
        /// <param name="other">The other transfer job to copy properties.</param>
        private TransferJob(TransferJob other)
        {
            this.Overwrite = other.Overwrite;
            this.CopyId = other.CopyId;
            this.CheckPoint = other.CheckPoint.Copy();
            this.Status = other.Status;
        }

        public ReaderWriterLockSlim ProgressUpdateLock
        {
            get;
            set;
        }

        /// <summary>
        /// Gets source location for this transfer job.
        /// </summary>
        public TransferLocation Source
        {
            get
            {
                return this.Transfer.Source;
            }
        }

        /// <summary>
        /// Gets destination location for this transfer job.
        /// </summary>
        public TransferLocation Destination
        {
            get
            {
                return this.Transfer.Destination;
            }
        }

        /// <summary>
        /// Gets or sets the overwrite flag.
        /// </summary>
        [DataMember]
        public bool? Overwrite
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets ID for the asynchronous copy operation.
        /// </summary>
        /// <value>ID for the asynchronous copy operation.</value>
        [DataMember]
        public string CopyId
        {
            get;
            set;
        }

        [DataMember]
        public TransferJobStatus Status
        {
            get;
            set;
        }

        [DataMember]
        public SingleObjectCheckpoint CheckPoint
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the parent transfer of this transfer job
        /// </summary>
        public SingleObjectTransfer Transfer
        {
            get;
            set;
        }
    
        /// <summary>
        /// Gets a copy of this transfer job.
        /// </summary>
        /// <returns>A copy of current transfer job</returns>
        public TransferJob Copy()
        {
            return new TransferJob(this);
        }
    }
}
