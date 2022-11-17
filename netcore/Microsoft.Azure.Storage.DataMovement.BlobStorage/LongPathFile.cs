//------------------------------------------------------------------------------
// <copyright file="LongPathFile.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
  using System.IO;

  internal static partial class LongPathFile
    {
        public static bool Exists(string path)
        {
            return File.Exists(path);
        }
    }
}
