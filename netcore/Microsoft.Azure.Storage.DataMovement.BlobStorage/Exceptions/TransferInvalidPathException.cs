namespace Microsoft.Azure.Storage.DataMovement
{
	using System;
	using System.Runtime.Serialization;

	/// <summary>
	/// Exceptions thrown when transfer skips.
	/// </summary>
	[DataContract]
	public class TransferInvalidPathException : TransferException
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="TransferInvalidPathException" /> class.
		/// </summary>
		public TransferInvalidPathException()
			: base(TransferErrorCode.PathCustomValidationFailed)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="TransferInvalidPathException" /> class.
		/// </summary>
		/// <param name="errorMessage">The message that describes the error.</param>
		public TransferInvalidPathException(string errorMessage)
			: base(TransferErrorCode.PathCustomValidationFailed, errorMessage)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="TransferInvalidPathException" /> class.
		/// </summary>
		/// <param name="errorMessage">Exception message.</param>
		/// <param name="innerException">Inner exception.</param>
		public TransferInvalidPathException(string errorMessage, Exception innerException)
			: base(TransferErrorCode.PathCustomValidationFailed, errorMessage, innerException)
		{
		}

	}
}