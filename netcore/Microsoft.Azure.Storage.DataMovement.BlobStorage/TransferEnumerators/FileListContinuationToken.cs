//------------------------------------------------------------------------------
// <copyright file="FileListContinuationToken.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
  using System.Runtime.Serialization;

    [DataContract]
    sealed class FileListContinuationToken : ListContinuationToken
    {
        private const string FilePathName = "FilePath";

        public FileListContinuationToken(string filePath)
        {
            this.FilePath = filePath;
        }

        /// <summary>
        /// Gets relative path of the last listed file.
        /// </summary>
        [DataMember]
        public string FilePath
        {
            get;
            private set;
        }
    }
}
