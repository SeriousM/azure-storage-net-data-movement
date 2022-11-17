//------------------------------------------------------------------------------
// <copyright file="DirectoryTransfer.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement.TransferEnumerators;

    /// <summary>
    /// Represents a directory object transfer operation.
    /// </summary>
    [KnownType(typeof(AzureBlobDirectoryLocation))]
    [KnownType(typeof(AzureBlobLocation))]
    [KnownType(typeof(DirectoryLocation))]
    [KnownType(typeof(FileLocation))]
    // StreamLocation intentionally omitted because it is not serializable
    [KnownType(typeof(UriLocation))]
    [KnownType(typeof(SingleObjectTransfer))]
    [KnownType(typeof(HierarchyDirectoryTransfer))]
    [KnownType(typeof(FlatDirectoryTransfer))]
    [DataContract]
    internal abstract class DirectoryTransfer : Transfer
    {
        /// <summary>
        /// Internal directory transfer context instance.
        /// </summary>
        private DirectoryTransferContext dirTransferContext = null;

        /// <summary>
        /// Serialization field name for bool to indicate whether delimiter is set.
        /// </summary>
        private const string HasDelimiterName = "HasDelimiter";

        /// <summary>
        /// Serialization field name for delimiter.
        /// </summary>
        private const string DelimiterName = "Delimiter";

        /// <summary>
        /// Name resolver.
        /// </summary>
        private INameResolver nameResolver;

        [DataMember]
        private char? delimiter = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransfer"/> class.
        /// </summary>
        /// <param name="source">Transfer source.</param>
        /// <param name="dest">Transfer destination.</param>
        /// <param name="transferMethod">Transfer method, see <see cref="TransferMethod"/> for detail available methods.</param>
        public DirectoryTransfer(TransferLocation source, TransferLocation dest, TransferMethod transferMethod)
            : base(source, dest, transferMethod)
        {
        }

        // Initialize a new DirectoryTransfer object after deserialization
        [OnDeserialized]
        private void OnDeserializedCallback(StreamingContext context)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryTransfer"/> class.
        /// </summary>
        /// <param name="other">Another <see cref="DirectoryTransfer"/> object.</param>
        protected DirectoryTransfer(DirectoryTransfer other)
            : base(other)
        {
        }

        protected INameResolver NameResolver
        {
            get
            {
                return this.nameResolver;
            }
        }

        public char? Delimiter
        {
            get
            {
                return this.delimiter;
            }

            set
            {
                this.delimiter = value;
            }
        }

        public bool IsForceOverwrite
        {
            get
            {
                if (this.DirectoryContext == null)
                {
                    return false;
                }

                return this.DirectoryContext.ShouldOverwriteCallbackAsync == TransferContext.ForceOverwrite;
            }
        }


        /// <summary>
        /// Gets or sets the transfer context of this transfer.
        /// </summary>
        public override TransferContext Context
        {
            get
            {
                return this.dirTransferContext;
            }

            set
            {
                var tempValue = value as DirectoryTransferContext;

                if (tempValue == null)
                {
                    throw new ArgumentException("Requires a DirectoryTransferContext instance", "value");
                }

                this.dirTransferContext = tempValue;
            }
        }

        /// <summary>
        /// Gets the directory transfer context of this transfer.
        /// </summary>
        public DirectoryTransferContext DirectoryContext
        {
            get
            {
                return this.dirTransferContext;
            }
        }

        /// <summary>
        /// Gets or sets the transfer enumerator for source location
        /// </summary>
        public ITransferEnumerator SourceEnumerator
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the maximum transfer concurrency
        /// </summary>
        public virtual int MaxTransferConcurrency
        {
            get;
            set;
        }


        /// <summary>
        /// Execute the transfer asynchronously.
        /// </summary>
        /// <param name="scheduler">Transfer scheduler</param>
        /// <param name="cancellationToken">Token that can be used to cancel the transfer.</param>
        /// <returns>A task representing the transfer operation.</returns>
        public override async Task ExecuteAsync(TransferScheduler scheduler, CancellationToken cancellationToken)
        {
            try
            {
                this.Destination.Validate();
            }
            catch (StorageException se)
            {
                throw new TransferException(TransferErrorCode.FailToVadlidateDestination,
                    Resources.FailedToValidateDestinationException,
                    se);
            }
            catch (Exception ex)
            {
                throw new TransferException(TransferErrorCode.FailToVadlidateDestination,
                    Resources.FailedToValidateDestinationException,
                    ex);
            }

            this.nameResolver = GetNameResolver(this.Source, this.Destination, this.Delimiter);
            await this.ExecuteInternalAsync(scheduler, cancellationToken).ConfigureAwait(false);
        }

        public abstract Task ExecuteInternalAsync(TransferScheduler scheduler, CancellationToken cancellationToken);

        protected static TransferLocation GetSourceTransferLocation(TransferLocation dirLocation, TransferEntry entry)
        {
            switch(dirLocation.Type)
            {
                case TransferLocationType.AzureBlobDirectory:
                    AzureBlobDirectoryLocation azureBlobDirLocation = dirLocation as AzureBlobDirectoryLocation;
                    AzureBlobEntry azureBlobEntry = entry as AzureBlobEntry;

                    AzureBlobLocation azureBlobLocation = new AzureBlobLocation(azureBlobEntry.Blob);
                    azureBlobLocation.BlobRequestOptions = azureBlobDirLocation.BlobRequestOptions;

                    return azureBlobLocation;
                case TransferLocationType.LocalDirectory:
                    FileEntry fileEntry = entry as FileEntry;

                    return new FileLocation(fileEntry.FullPath, fileEntry.RelativePath);
                default:
                    throw new ArgumentException("TransferLocationType");
            }
        }

        protected TransferLocation GetDestTransferLocationForEmptyDir(TransferLocation dirLocation, TransferEntry entry)
        {
            string destRelativePath = this.nameResolver.ResolveName(entry);

            AzureBlobEntry sourceBlobEntry = entry as AzureBlobEntry;

            switch (dirLocation.Type)
            {
                case TransferLocationType.AzureBlobDirectory:
                    {
                        return null;
                    }

                case TransferLocationType.LocalDirectory:
                    {
                        return null;
                    }

                default:
                    throw new ArgumentException("TransferLocationType");
            }
        }

        protected TransferLocation GetDestinationTransferLocation(TransferLocation dirLocation, TransferEntry entry)
        {
            string destRelativePath = this.nameResolver.ResolveName(entry);

            AzureBlobEntry sourceBlobEntry = entry as AzureBlobEntry;

            switch (dirLocation.Type)
            {
                case TransferLocationType.AzureBlobDirectory:
                    {
                        AzureBlobDirectoryLocation blobDirLocation = dirLocation as AzureBlobDirectoryLocation;
                        BlobType destBlobType = this.BlobType;

                        if (sourceBlobEntry != null)
                        {
                            // if source is Azure blob storage, source and destination blob share the same blob type
                            destBlobType = sourceBlobEntry.Blob.BlobType;
                        }

                        CloudBlob blob = null;
                        switch (destBlobType)
                        {
                            case Blob.BlobType.BlockBlob:
                            case Blob.BlobType.Unspecified:
                                blob = blobDirLocation.BlobDirectory.GetBlockBlobReference(destRelativePath);
                                break;

                            case Blob.BlobType.PageBlob:
                                blob = blobDirLocation.BlobDirectory.GetPageBlobReference(destRelativePath);
                                break;

                            case Blob.BlobType.AppendBlob:
                                blob = blobDirLocation.BlobDirectory.GetAppendBlobReference(destRelativePath);
                                break;
                        }

                        AzureBlobLocation retLocation = new AzureBlobLocation(blob);
                        retLocation.BlobRequestOptions = blobDirLocation.BlobRequestOptions;
                        return retLocation;
                    }

                case TransferLocationType.LocalDirectory:
                    {
                        DirectoryLocation localDirLocation = dirLocation as DirectoryLocation;
                        string path = Path.Combine(localDirLocation.DirectoryPath, destRelativePath);
                        
                        return new FileLocation(path, destRelativePath);
                    }

                default:
                    throw new ArgumentException("TransferLocationType");
            }
        }

        public void CreateParentDirectory(SingleObjectTransfer transfer)
        {
            switch (transfer.Destination.Type)
            {
                case TransferLocationType.FilePath:
                    var filePath = (transfer.Destination as FileLocation).FilePath;
                    Utils.ValidateDestinationPath(transfer.Source.Instance.ConvertToString(), filePath);
                    filePath = filePath.ToLongPath();
                    Utils.CreateParentDirectoryIfNotExists(filePath);
                    break;
                default:
                    break;
            }
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Object will be disposed by the caller")]
        internal SingleObjectTransfer CreateTransfer(TransferEntry entry)
        {
            TransferLocation sourceLocation = GetSourceTransferLocation(this.Source, entry);
            sourceLocation.IsInstanceInfoFetched = true;
            TransferLocation destLocation = GetDestinationTransferLocation(this.Destination, entry);
            var transferMethod = IsDummyCopy(entry) ? TransferMethod.DummyCopy : this.TransferMethod;
            SingleObjectTransfer transfer = new SingleObjectTransfer(sourceLocation, destLocation, transferMethod);
            transfer.Context = this.Context;
            return transfer;
        }

        private bool IsDummyCopy(TransferEntry entry)
        {
            if (this.Source.Type == TransferLocationType.AzureBlobDirectory
                && this.Destination.Type == TransferLocationType.LocalDirectory)
            {
                if(IsDirectoryBlob((entry as AzureBlobEntry)?.Blob))
                {
                    return true;
                }
            }
            return false;
        }

        private static bool IsDirectoryBlob(CloudBlob blob)
        {
            if (blob != null)
            {
                if (blob.Properties.Length == 0)
                {
                    foreach (var metadata in blob.Metadata)
                    {
                        if (String.Compare(metadata.Key, Constants.DirectoryBlobMetadataKey, StringComparison.OrdinalIgnoreCase) == 0
                            && String.Compare(metadata.Value, "true", StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        protected void UpdateTransfer(Transfer transfer)
        {
            DirectoryTransfer.UpdateCredentials(this.Source, transfer.Source);
            DirectoryTransfer.UpdateCredentials(this.Destination, transfer.Destination);
        }

        private static void UpdateCredentials(TransferLocation dirLocation, TransferLocation subLocation)
        {
            if (dirLocation.Type == TransferLocationType.AzureBlobDirectory)
            {
                AzureBlobDirectoryLocation blobDirectoryLocation = dirLocation as AzureBlobDirectoryLocation;
                (subLocation as AzureBlobLocation).UpdateCredentials(blobDirectoryLocation.BlobDirectory.ServiceClient.Credentials);
            }
        }

        private static INameResolver GetNameResolver(TransferLocation sourceLocation, TransferLocation destLocation, char? delimiter)
        {
            Debug.Assert(sourceLocation != null, "sourceLocation");
            Debug.Assert(destLocation != null, "destLocation");

            switch (sourceLocation.Type)
            {
                case TransferLocationType.AzureBlobDirectory:
                    if (destLocation.Type == TransferLocationType.AzureBlobDirectory)
                    {
                        return new AzureBlobToAzureBlobNameResolver();
                    }
                    else if (destLocation.Type == TransferLocationType.LocalDirectory)
                    {
                        return new AzureToFileNameResolver(delimiter);
                    }
                    break;

                case TransferLocationType.LocalDirectory:
                    if (destLocation.Type == TransferLocationType.AzureBlobDirectory)
                    {
                        return new FileToAzureBlobNameResolver();
                    }
                    break;

                default:
                    throw new ArgumentException("Unsupported source location", "sourceLocation");
            }

            throw new ArgumentException("Unsupported destination location", "destLocation");
        }

        private static string AppendSlash(string input)
        {
            if (input.EndsWith("/", StringComparison.Ordinal))
            {
                return input;
            }
            else
            {
                return input + "/";
            }
        }
    }
}
