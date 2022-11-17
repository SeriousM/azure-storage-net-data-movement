//------------------------------------------------------------------------------
// <copyright file="TransferException.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Base exception class for exceptions thrown by Blob/FileTransferJobs.
    /// </summary>
    [DataContract]
    public class TransferException : Exception
    {
        /// <summary>
        /// Transfer error code.
        /// </summary>
        [DataMember]
        private TransferErrorCode errorCode;

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        public TransferException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public TransferException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="ex">The exception that is the cause of the current exception, or a null reference
        /// if no inner exception is specified.</param>
        public TransferException(string message, Exception ex)
            : base(message, ex)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="errorCode">Transfer error code.</param>
        public TransferException(TransferErrorCode errorCode)
        {
            this.errorCode = errorCode;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="errorCode">Transfer error code.</param>
        /// <param name="message">Exception message.</param>
        public TransferException(
            TransferErrorCode errorCode, 
            string message)
            : base(message)
        {
            this.errorCode = errorCode;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferException" /> class.
        /// </summary>
        /// <param name="errorCode">Transfer error code.</param>
        /// <param name="message">Exception message.</param>
        /// <param name="innerException">Inner exception.</param>
        public TransferException(
            TransferErrorCode errorCode, 
            string message, 
            Exception innerException)
            : base(message, innerException)
        {
            this.errorCode = errorCode;
        }

        /// <summary>
        /// Gets the detailed error code.
        /// </summary>
        /// <value>The error code of the exception.</value>
        public TransferErrorCode ErrorCode
        {
            get
            {
                return this.errorCode;
            }
        }

    }
}
