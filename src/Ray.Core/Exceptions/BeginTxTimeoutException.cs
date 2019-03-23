using System;

namespace Ray.Core.Exceptions
{
    public class BeginTxTimeoutException : Exception
    {
        public BeginTxTimeoutException(string stateId, long transactionId, Type type) :
            base($"Grain type {type.FullName} with grainId {stateId} and transactionId {transactionId}")
        {
        }
    }
}
