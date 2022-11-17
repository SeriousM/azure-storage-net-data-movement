//------------------------------------------------------------------------------
// <copyright file="ListContinuationToken.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.TransferEnumerators
{
  using System.Runtime.Serialization;

    /// <summary>
    /// Base class of list continuation tokens
    /// </summary>
    [DataContract]
    internal abstract class ListContinuationToken
    {
    }
}
