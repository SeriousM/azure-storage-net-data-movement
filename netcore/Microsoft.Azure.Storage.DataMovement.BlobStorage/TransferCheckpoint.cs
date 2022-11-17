//------------------------------------------------------------------------------
// <copyright file="TransferCheckpoint.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
  using System.IO;
    using System.Runtime.Serialization;

    /// <summary>
    /// Represents a checkpoint from which a transfer may be resumed and continue.
    /// </summary>
    [DataContract]
    public class TransferCheckpoint
    {
        private const string TransferCollectionName = "TransferCollection";

        private StreamJournal Journal = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCheckpoint"/> class.
        /// </summary>
        /// <param name="other">Another TransferCheckpoint object. </param>
        internal TransferCheckpoint(TransferCheckpoint other)
        {
            if (null == other)
            {
                this.TransferCollection = new TransferCollection<Transfer>();
            }
            else
            { 
                this.TransferCollection = other.TransferCollection.Copy();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferCheckpoint"/> class.
        /// </summary>
        /// <param name="journalStream">Stream to write checkpoint journal to. </param>
        /// <param name="disableJournalValidation">A flag that indicates whether to validate an assembly version serialized in a journal stream or not.</param>
        internal TransferCheckpoint(Stream journalStream, bool disableJournalValidation)
        {
            this.TransferCollection = new TransferCollection<Transfer>();
            this.Journal = new StreamJournal(journalStream, disableJournalValidation);
            Transfer transferInstance = this.Journal.Initialize();

            if (null != transferInstance)
            {
                this.TransferCollection.AddTransfer(transferInstance);
            }
        }

        /// <summary>
        /// Gets that container that tracks all transfers associated with this transfer checkpoint
        /// </summary>

        [DataMember]
        internal TransferCollection<Transfer> TransferCollection
        {
            get;
            private set;
        }

        /// <summary>
        /// Adds a transfer to the transfer checkpoint.
        /// </summary>
        /// <param name="transfer">The transfer to be kept track of.</param>
        internal void AddTransfer(Transfer transfer)
        {
            this.Journal?.AddTransfer(transfer);
            this.TransferCollection.AddTransfer(transfer);
        }

        /// <summary>
        /// Gets a transfer with the specified source location, destination location and transfer method.
        /// </summary>
        /// <param name="sourceLocation">Source location of the transfer.</param>
        /// <param name="destLocation">Destination location of the transfer.</param>
        /// <param name="transferMethod">Transfer method.</param>
        /// <returns>A transfer that matches the specified source location, destination location and transfer method; Or null if no matches.</returns>
        internal Transfer GetTransfer(TransferLocation sourceLocation, TransferLocation destLocation, TransferMethod transferMethod)
        {
            return this.TransferCollection.GetTransfer(sourceLocation, destLocation, transferMethod);
        }

        /// <summary>
        /// Gets a static snapshot of this transfer checkpoint
        /// </summary>
        /// <returns>A snapshot of current transfer checkpoint</returns>
        internal TransferCheckpoint Copy()
        {
            return new TransferCheckpoint(this);
        }
    }
}
