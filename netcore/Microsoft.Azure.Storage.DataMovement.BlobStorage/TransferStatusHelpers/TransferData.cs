//------------------------------------------------------------------------------
// <copyright file="TransferData.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System.IO;

    internal class TransferData : TransferDataState
    {
        private MemoryManager memoryManager;

        public TransferData(MemoryManager memoryManager)
        {
            this.memoryManager = memoryManager;
        }

        public Stream Stream
        {
            get;
            set;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (null != this.Stream)
                {
                    this.Stream.Dispose();
                    this.Stream = null;
                }

                if (null != this.MemoryBuffer)
                {
                    this.memoryManager.ReleaseBuffers(this.MemoryBuffer);
                    this.MemoryBuffer = null;
                }
            }
        }
    }
}
