﻿//------------------------------------------------------------------------------
// <copyright file="TransferSkippedException.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Exceptions thrown when transfer skips.
    /// </summary>
    [DataContract]
    public class TransferSkippedException : TransferException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransferSkippedException" /> class.
        /// </summary>
        public TransferSkippedException()
            : base(TransferErrorCode.NotOverwriteExistingDestination)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferSkippedException" /> class.
        /// </summary>
        /// <param name="errorMessage">The message that describes the error.</param>
        public TransferSkippedException(string errorMessage)
            : base(TransferErrorCode.NotOverwriteExistingDestination, errorMessage)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransferSkippedException" /> class.
        /// </summary>
        /// <param name="errorMessage">Exception message.</param>
        /// <param name="innerException">Inner exception.</param>
        public TransferSkippedException(string errorMessage, Exception innerException)
            : base(TransferErrorCode.NotOverwriteExistingDestination, errorMessage, innerException)
        {
        }

    }
}
