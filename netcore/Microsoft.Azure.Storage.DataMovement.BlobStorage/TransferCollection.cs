//------------------------------------------------------------------------------
// <copyright file="TransferCollection.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.Azure.Storage.DataMovement
{
  using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
  using System.Runtime.Serialization;
    using TransferKey = System.Tuple<TransferLocation, TransferLocation>;

    /// <summary>
    /// A collection of transfers.
    /// </summary>
    [DataContract]
    [KnownType(typeof(DirectoryTransfer))]
    [KnownType(typeof(SingleObjectTransfer))]
    internal class TransferCollection<T>
    {
        /// <summary>
        /// Serialization field name for single object transfers.
        /// </summary>
        private const string SingleObjectTransfersName = "SingleObjectTransfers";

        /// <summary>
        /// Serialization field name for directory transfers.
        /// </summary>
        private const string DirectoryTransfersName = "DirectoryTransfers";

        /// <summary>
        /// All transfers in the collection.
        /// </summary>
        private ConcurrentDictionary<TransferKey, Transfer> transfers = new ConcurrentDictionary<TransferKey, Transfer>();

        /// <summary>
        /// Overall transfer progress tracker.
        /// </summary>
        private TransferProgressTracker overallProgressTracker = new TransferProgressTracker();

#region Serialization helpers

        [DataMember]
        private Transfer[] serializedTransfers;

        /// <summary>
        /// Initializes a deserialized TransferCollection (by rebuilding the the transfer
        /// dictionary and progress tracker)
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
            // DCS doesn't invoke ctors, so all initialization must be done here
            transfers = new ConcurrentDictionary<TransferKey, Transfer>();
            overallProgressTracker = new TransferProgressTracker();

            foreach (Transfer t in serializedTransfers)
            {
                this.AddTransfer(t);
            }

            foreach (Transfer transfer in this.transfers.Values)
            {
                this.OverallProgressTracker.AddProgress(transfer.ProgressTracker);
            }
        }

        /// <summary>
        /// Serializes the object by storing the trasnfers in a more DCS-friendly format
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        private void OnSerializingCallback(StreamingContext context)
        {
            serializedTransfers = this.transfers.Select(kv => kv.Value).Where(t => t != null).ToArray();
        }
#endregion // Serialization helpers

        /// <summary>
        /// Gets the number of transfers currently in the collection.
        /// </summary>
        public int Count
        {
            get
            {
                return this.transfers.Count;
            }
        }

        /// <summary>
        /// Gets the overall transfer progress.
        /// </summary>
        public TransferProgressTracker OverallProgressTracker
        {
            get
            {
                return this.overallProgressTracker;
            }
        }

        /// <summary>
        /// Adds a transfer.
        /// </summary>
        /// <param name="transfer">The transfer to be added.</param>
        /// <param name="updateProgress">Whether or not to update collection's progress with the subtransfer's.</param>
        public void AddTransfer(Transfer transfer, bool updateProgress = true)
        {
            transfer.ProgressTracker.Parent = this.OverallProgressTracker;

            if (updateProgress)
            {
                this.overallProgressTracker.AddProgress(transfer.ProgressTracker);
            }

            bool unused = this.transfers.TryAdd(new TransferKey(transfer.Source, transfer.Destination), transfer);
            Debug.Assert(unused, "Transfer with the same source and destination already exists");
        }

        /// <summary>
        /// Remove a transfer.
        /// </summary>
        /// <param name="transfer">Transfer to be removed</param>
        /// <returns>True if the transfer is removed successfully, false otherwise.</returns>
        public bool RemoveTransfer(Transfer transfer)
        {
            Transfer unused = null;
            if (this.transfers.TryRemove(new TransferKey(transfer.Source, transfer.Destination), out unused))
            {
                transfer.ProgressTracker.Parent = null;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Gets a transfer with the specified source location, destination location and transfer method.
        /// </summary>
        /// <param name="sourceLocation">Source location of the transfer.</param>
        /// <param name="destLocation">Destination location of the transfer.</param>
        /// <param name="transferMethod">Transfer method.</param>
        /// <returns>A transfer that matches the specified source location, destination location and transfer method; Or null if no matches.</returns>
        public Transfer GetTransfer(TransferLocation sourceLocation, TransferLocation destLocation, TransferMethod transferMethod)
        {
            Transfer transfer = null;
            if (this.transfers.TryGetValue(new TransferKey(sourceLocation, destLocation), out transfer))
            {
                if (transfer.TransferMethod == transferMethod)
                {
                    return transfer;
                }
            }

            return null;
        }

        /// <summary>
        /// Get an enumerable object for all tansfers in this TransferCollection.
        /// </summary>
        /// <returns>An enumerable object for all tansfers in this TransferCollection.</returns>
        public IEnumerable<Transfer> GetEnumerator()
        {
            return this.transfers.Values;
        }

        /// <summary>
        /// Gets a static snapshot of this transfer checkpoint
        /// </summary>
        /// <returns>A snapshot of current transfer checkpoint</returns>
        public TransferCollection<T> Copy()
        {
            TransferCollection<T> copyObj = new TransferCollection<T>();
            foreach (var kv in this.transfers)
            {
                Transfer transfer = kv.Value as Transfer;
                copyObj.AddTransfer((Transfer)transfer.Copy());
            }

            return copyObj;
        }
    }
}
