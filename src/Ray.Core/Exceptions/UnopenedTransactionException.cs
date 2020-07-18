using System;

namespace Ray.Core.Exceptions
{
    public class UnopenedTransactionException : Exception
    {
        public UnopenedTransactionException(string id, Type grainType, string methodName) :
            base($"Unopened transaction, cannot be invoke {methodName},type {grainType.FullName} with id {id}")
        {
        }
    }
}
