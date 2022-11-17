//------------------------------------------------------------------------------
// <copyright file="SubDirectoryTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.DataMovement.TransferEnumerators;

    /// <summary>
    /// Represents a sub-directory transfer under a hierarchy directory transfer.
    /// </summary>
    [DataContract]
    class SubDirectoryTransfer : JournalItem
    {
        private const string SubDirListContinuationTokenName = "SubDirListContinuationToken";
        private const string SubDirRelativePathName = "SubDirRelativePath";

        /// <summary>
        /// Base <see cref="HierarchyDirectoryTransfer"/> instance which this <see cref="SubDirectoryTransfer"/> instance belongs to.
        /// <see cref="SubDirectoryTransfer"/> instance returns its listed directories and files to <see cref="HierarchyDirectoryTransfer"/> with callbacks.
        /// </summary>
        private HierarchyDirectoryTransfer baseDirectoryTransfer = null;
        private ITransferEnumerator transferEnumerator = null;

        [DataMember]
        private string relativePath = null;

        [DataMember]
        private SerializableListContinuationToken enumerateContinuationToken = null;

        private TransferLocation source;
        private TransferLocation dest;

        public SubDirectoryTransfer(
            HierarchyDirectoryTransfer baseDirectoryTransfer,
            string relativePath)
        {
            this.enumerateContinuationToken = new SerializableListContinuationToken(null);
            this.baseDirectoryTransfer = baseDirectoryTransfer;
            this.relativePath = relativePath;
            this.baseDirectoryTransfer.GetSubDirLocation(this.relativePath, out this.source, out this.dest);
            this.InitializeEnumerator();
        }

        public SubDirectoryTransfer(SubDirectoryTransfer other)
        {
            this.relativePath = other.relativePath;
            this.enumerateContinuationToken = other.enumerateContinuationToken;
        }

        public SerializableListContinuationToken ListContinuationToken
        {
            get
            {
                return this.enumerateContinuationToken;
            }

            set
            {
                this.enumerateContinuationToken = value;
            }
        }

        public TransferLocation Source
        {
            get
            {
                return this.source;
            }
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            await Task.Yield();

            this.CreateDestinationDirectory(cancellationToken);

            var enumerator = this.transferEnumerator.EnumerateLocation(cancellationToken).GetEnumerator();

            while (true)
            {
                if (!enumerator.MoveNext())
                {
                    break;
                }

                TransferEntry entry = enumerator.Current;
                var errorEntry = entry as ErrorEntry;
                if (errorEntry != null)
                {
                    TransferException exception = errorEntry.Exception as TransferException;
                    if (null != exception)
                    {
                        throw exception;
                    }
                    else
                    {
                        throw new TransferException(
                            TransferErrorCode.FailToEnumerateDirectory,
                            errorEntry.Exception.GetExceptionMessage(),
                            errorEntry.Exception);
                    }
                }

                if (entry.IsDirectory)
                {
                    this.baseDirectoryTransfer.AddSubDir(entry.RelativePath, () =>
                    {
                        var currentContinuationToken = new SerializableListContinuationToken(entry.ContinuationToken);
                        currentContinuationToken.Journal = this.enumerateContinuationToken.Journal;
                        currentContinuationToken.StreamJournalOffset = this.enumerateContinuationToken.StreamJournalOffset;
                        this.enumerateContinuationToken = currentContinuationToken;
                        return this.enumerateContinuationToken;
                    });
                }
                else
                {
                    SingleObjectTransfer transferItem = this.baseDirectoryTransfer.CreateTransfer(entry);
#if DEBUG
                    Utils.HandleFaultInjection(entry.RelativePath, transferItem);
#endif

                    this.CreateDestinationParentDirectoryRecursively(transferItem);

                    this.baseDirectoryTransfer.AddSingleObjectTransfer(transferItem, () =>
                    {
                        var currentContinuationToken = new SerializableListContinuationToken(entry.ContinuationToken);
                        currentContinuationToken.Journal = this.enumerateContinuationToken.Journal;
                        currentContinuationToken.StreamJournalOffset = this.enumerateContinuationToken.StreamJournalOffset;
                        this.enumerateContinuationToken = currentContinuationToken;
                        return this.enumerateContinuationToken;
                    });
                }
            }
        }

        public void Update(HierarchyDirectoryTransfer baseDirectoryTransferInstance)
        {
            this.baseDirectoryTransfer = baseDirectoryTransferInstance;
            this.baseDirectoryTransfer.GetSubDirLocation(this.relativePath, out this.source, out this.dest);
            this.InitializeEnumerator();
        }

        public void CreateDestinationParentDirectoryRecursively(SingleObjectTransfer transferItem)
        {
            switch (transferItem.Destination.Type)
            {
                case TransferLocationType.FilePath:
                    var filePath = (transferItem.Destination as FileLocation).FilePath;
                    Utils.ValidateDestinationPath(transferItem.Source.Instance.ConvertToString(), filePath);
                    Utils.CreateParentDirectoryIfNotExists(filePath.ToLongPath());
                    break;
                default:
                    break;
            }
        }

        private void InitializeEnumerator()
        {
            if (this.source.Type == TransferLocationType.LocalDirectory)
            {
                var fileEnumerator = new FileHierarchyEnumerator(this.source as DirectoryLocation, this.baseDirectoryTransfer.Source.Instance as string, this.baseDirectoryTransfer.FollowSymblink);
                fileEnumerator.EnumerateContinuationToken = this.enumerateContinuationToken.ListContinuationToken;
                fileEnumerator.SearchPattern = this.baseDirectoryTransfer.SearchPattern;
                fileEnumerator.Recursive = this.baseDirectoryTransfer.Recursive;

                this.transferEnumerator = fileEnumerator;
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        private void CreateDestinationDirectory(CancellationToken cancellationToken)
        {
            if (this.dest.Type == TransferLocationType.AzureBlobDirectory)
            {
                // No physical destination directory needed.
                return;
            }

            if (this.dest.Type == TransferLocationType.LocalDirectory)
            {
                string directoryPath = (this.dest as DirectoryLocation).DirectoryPath.ToLongPath();
                
                if (!LongPathDirectory.Exists(directoryPath))
                {
                    LongPathDirectory.CreateDirectory(directoryPath);
                }
            }
        }
    }
}
