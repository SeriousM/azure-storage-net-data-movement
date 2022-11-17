//------------------------------------------------------------------------------
// <copyright file="TransferProgressTracker.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Runtime.Serialization;
    using System.Threading;

    /// <summary>
    /// Calculate transfer progress.
    /// </summary>
    [DataContract]
    internal class TransferProgressTracker : JournalItem
    {
        private const string BytesTransferredName = "BytesTransferred";
        private const string FilesTransferredName = "FilesTransferred";
        private const string FilesSkippedName = "FilesSkipped";
        private const string FilesFailedName = "FilesFailed";

        /// <summary>
        /// Stores the number of bytes that have been transferred.
        /// </summary>
        [DataMember]
        private long bytesTransferred;

        /// <summary>
        /// Stores the number of files that have been transferred.
        /// </summary>
        [DataMember]
        private long numberOfFilesTransferred;

        /// <summary>
        /// Stores the number of files that are failed to be transferred.
        /// </summary>
        [DataMember]
        private long numberOfFilesSkipped;

        /// <summary>
        /// Stores the number of files that are skipped.
        /// </summary>
        [DataMember]
        private long numberOfFilesFailed;

        /// <summary>
        /// A flag indicating whether the progress handler is being invoked
        /// </summary>
        private int invokingProgressHandler;

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferProgressTracker" /> class.
        /// </summary>
        public TransferProgressTracker()
        {
            this.bytesTransferred = 0;
            this.numberOfFilesTransferred = 0;
            this.numberOfFilesSkipped = 0;
            this.numberOfFilesFailed = 0;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferProgressTracker" /> class.
        /// </summary>
        private TransferProgressTracker(TransferProgressTracker other)
        {
            this.bytesTransferred = other.BytesTransferred;
            this.numberOfFilesTransferred = other.NumberOfFilesTransferred;
            this.numberOfFilesSkipped = other.NumberOfFilesSkipped;
            this.numberOfFilesFailed = other.NumberOfFilesFailed;
        }

        /// <summary>
        /// Gets or sets the parent progress tracker
        /// </summary>
        public TransferProgressTracker Parent
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the progress handler
        /// </summary>
        public IProgress<TransferStatus> ProgressHandler
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the number of bytes that have been transferred.
        /// </summary>
        public long BytesTransferred
        {
            get
            {
                return Interlocked.Read(ref this.bytesTransferred);
            }
        }

        /// <summary>
        /// Gets the number of files that have been transferred.
        /// </summary>
        public long NumberOfFilesTransferred
        {
            get
            {
                return Interlocked.Read(ref this.numberOfFilesTransferred);
            }

        }

        /// <summary>
        /// Gets the number of files that are skipped to be transferred.
        /// </summary>
        public long NumberOfFilesSkipped
        {
            get
            {
                return Interlocked.Read(ref this.numberOfFilesSkipped);
            }
        }

        /// <summary>
        /// Gets the number of files that are failed to be transferred.
        /// </summary>
        public long NumberOfFilesFailed
        {
            get
            {
                return Interlocked.Read(ref this.numberOfFilesFailed);
            }
        }

        /// <summary>
        /// Updates the current status by indicating the bytes transferred.
        /// </summary>
        /// <param name="bytesToIncrease">Indicating by how much the bytes transferred increased.</param>
        public void AddBytesTransferred(long bytesToIncrease)
        {
            if (bytesToIncrease != 0)
            {
                Interlocked.Add(ref this.bytesTransferred, bytesToIncrease);

                if (this.Parent != null)
                {
                    this.Parent.AddBytesTransferred(bytesToIncrease);
                }
            }

            this.InvokeProgressHandler();
        }

        /// <summary>
        /// Updates the number of files that have been transferred.
        /// </summary>
        /// <param name="numberOfFilesToIncrease">Indicating by how much the number of file that have been transferred increased.</param>
        public void AddNumberOfFilesTransferred(long numberOfFilesToIncrease)
        {
            if (numberOfFilesToIncrease != 0)
            {
                Interlocked.Add(ref this.numberOfFilesTransferred, numberOfFilesToIncrease);

                if (this.Parent != null)
                {
                    this.Parent.AddNumberOfFilesTransferred(numberOfFilesToIncrease);
                }
            }

            this.InvokeProgressHandler();
        }

        /// <summary>
        /// Updates the number of files that are skipped.
        /// </summary>
        /// <param name="numberOfFilesToIncrease">Indicating by how much the number of file that are skipped increased.</param>
        public void AddNumberOfFilesSkipped(long numberOfFilesToIncrease)
        {
            if (numberOfFilesToIncrease != 0)
            {
                Interlocked.Add(ref this.numberOfFilesSkipped, numberOfFilesToIncrease);

                if (this.Parent != null)
                {
                    this.Parent.AddNumberOfFilesSkipped(numberOfFilesToIncrease);
                }
            }

            this.InvokeProgressHandler();
        }

        /// <summary>
        /// Updates the number of files that are failed to be transferred.
        /// </summary>
        /// <param name="numberOfFilesToIncrease">Indicating by how much the number of file that are failed to be transferred increased.</param>
        public void AddNumberOfFilesFailed(long numberOfFilesToIncrease)
        {
            if (numberOfFilesToIncrease != 0)
            {
                Interlocked.Add(ref this.numberOfFilesFailed, numberOfFilesToIncrease);
                if (this.Parent != null)
                {
                    this.Parent.AddNumberOfFilesFailed(numberOfFilesToIncrease);
                }
            }

            this.InvokeProgressHandler();
        }

        public void AddProgress(TransferProgressTracker progressTracker)
        {
            this.AddBytesTransferred(progressTracker.BytesTransferred);
            this.AddNumberOfFilesFailed(progressTracker.NumberOfFilesFailed);
            this.AddNumberOfFilesSkipped(progressTracker.NumberOfFilesSkipped);
            this.AddNumberOfFilesTransferred(progressTracker.NumberOfFilesTransferred);
        }

        /// <summary>
        /// Gets a copy of this transfer progress tracker object.
        /// </summary>
        /// <returns>A copy of current TransferProgressTracker object</returns>
        public TransferProgressTracker Copy()
        {
            return new TransferProgressTracker(this);
        }

        private void InvokeProgressHandler()
        {
            this.Journal?.UpdateJournalItem(this);

            if (this.ProgressHandler != null)
            {
                if (0 == Interlocked.CompareExchange(ref this.invokingProgressHandler, 1, 0))
                {
                    lock (this.ProgressHandler)
                    {
                        Interlocked.Exchange(ref this.invokingProgressHandler, 0);

                        this.ProgressHandler.Report(
                            new TransferStatus()
                            {
                                BytesTransferred = this.BytesTransferred,
                                NumberOfFilesTransferred = this.NumberOfFilesTransferred,
                                NumberOfFilesSkipped = this.NumberOfFilesSkipped,
                                NumberOfFilesFailed = this.NumberOfFilesFailed,
                            });
                    }
                }
            }
        }
    }
}
