using System;

namespace Ray.Core.Exceptions
{
    public class RepeatedTransactionException : Exception
    {
        public RepeatedTransactionException(string id, Type type) :
            base($"Transactions of Grain type {type.FullName} and Id {id} are opened repeatedly")
        {
        }
    }
}
