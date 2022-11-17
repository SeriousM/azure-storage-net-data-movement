//------------------------------------------------------------------------------
// <copyright file="JournalItem.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{

    using System.Runtime.Serialization;
    [DataContract]
    internal abstract class JournalItem
    {
        public StreamJournal Journal
        {
            get;
            set;
        }

        public long StreamJournalOffset { get; set; }
    }
}
