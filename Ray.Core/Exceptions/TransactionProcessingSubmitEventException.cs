using System;

namespace Ray.Core.Exceptions
{
    public class TransactionProcessingSubmitEventException : Exception
    {
        public TransactionProcessingSubmitEventException(string id, Type grainType) : base($"In transaction processing, events cannot be submitted,type {grainType.FullName} with id {id}")
        {

        }
    }
}
