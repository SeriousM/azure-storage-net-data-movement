//------------------------------------------------------------------------------
// <copyright file="DMLibInputHelper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    public static class DMLibInputHelper
    {
        public static ShouldOverwriteCallbackAsync GetDefaultOverwiteCallbackY()
        {
            return async (source, dest) =>
            {
                return await Task.Run<bool>(() =>
                {
                    Thread.Sleep((new Random()).Next(100, 500));
                    Test.Info("Overwrite true: {0} -> {1}", DMLibTestHelper.TransferInstanceToString(source), DMLibTestHelper.TransferInstanceToString(dest));
                    return true;
                });
            };
        }

        public static ShouldOverwriteCallbackAsync GetDefaultOverwiteCallbackN()
        {
            return async (source, dest) =>
            {
                return await Task.Run<bool>(() =>
                {
                    Thread.Sleep((new Random()).Next(100, 500));
                    Test.Info("Overwrite false: {0} -> {1}", DMLibTestHelper.TransferInstanceToString(source), DMLibTestHelper.TransferInstanceToString(dest));
                    return false;
                });
            };
        }

        public static ValidatePathCallbackAsync GetDefaultFailedPathValidationCallback()
        {
	        return async source =>
	        {
		        await Task.Run(() =>
		        {
			        var error = $"Validation for path: {DMLibTestHelper.TransferInstanceToString(source)} should fail.";

                    Test.Info(error);
                    throw new Exception(error);
		        });
	        };
        }

        public static ValidatePathCallbackAsync GetDefaultSucceededPathValidationCallback()
        {
	        return async source =>
	        {
		        await Task.Run(() =>
		        {
			        Test.Info("Validation for path: {0} should pass.", DMLibTestHelper.TransferInstanceToString(source));
		        });
	        };
        }
    }
}
