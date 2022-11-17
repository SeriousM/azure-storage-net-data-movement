using System;
using System.IO;

namespace Microsoft.Azure.Storage.DataMovement
{
#if DEBUG
    public static class TestHookCallbacks
    {
        public static Action<string, FileAttributes> SetFileAttributesCallback;
        public static Func<string, FileAttributes> GetFileAttributesCallback;

        public static bool UnderTesting
        {
            get;
            set;
        }
    }
#endif
}
