//------------------------------------------------------------------------------
// <copyright file="FileEnumerator.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement.Interop
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Security;

    internal static partial class NativeMethods
    {
        private const string CORE_FILE_APIS = "kernel32.dll";
        private const string CORE_SYSINFO_APIS = "kernel32.dll";

        #region P/Invokes
        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport(CORE_SYSINFO_APIS, CharSet = CharSet.Unicode, SetLastError = true)]
        internal static extern bool GlobalMemoryStatusEx([In, Out] MEMORYSTATUSEX lpBuffer);

        [DllImport(CORE_FILE_APIS, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool FindClose(SafeHandle findFileHandle);

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode), BestFitMapping(false)]
        public struct WIN32_FIND_DATA
        {
            public FileAttributes FileAttributes;
            public System.Runtime.InteropServices.ComTypes.FILETIME CreationTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME LastAccessTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME LastWriteTime;
            public uint FileSizeHigh;
            public uint FileSizeLow;
            public int Reserved0;
            public int Reserved1;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 260)]
            public string FileName;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 14)]
            public string AlternateFileName;
        }

        public sealed class SafeFindHandle : SafeHandle
        {
            [SecurityCritical]
            internal SafeFindHandle()
                : base(IntPtr.Zero, true)
            {
            }

            public override bool IsInvalid
            {
                get { return handle == IntPtr.Zero || handle == (IntPtr)(-1); }
            }

            protected override bool ReleaseHandle()
            {
                if (!(this.IsInvalid || this.IsClosed))
                {
                    return NativeMethods.FindClose(this);
                }

                return this.IsInvalid || this.IsClosed;
            }

            protected override void Dispose(bool disposing)
            {
                if (!(this.IsInvalid || this.IsClosed))
                {
                    NativeMethods.FindClose(this);
                }

                base.Dispose(disposing);
            }
        }
        #endregion // P/Invokes

        #region Helper structs
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        internal class MEMORYSTATUSEX
        {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;

            public MEMORYSTATUSEX()
            {
                this.dwLength = (uint)Marshal.SizeOf<MEMORYSTATUSEX>();
            }
        }
        #endregion // Helper structs
    }
}