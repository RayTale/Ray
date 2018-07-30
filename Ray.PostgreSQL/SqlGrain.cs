using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.PostgreSQL
{
    public abstract class SqlGrain<K, S, W> : ESGrain<K, S, W>, ISqlGrain
    where S : class, IState<K>, new()
    where W : IMessageWrapper, new()
    {
        public abstract SqlGrainConfig GrainConfig { get; }
    }
    public abstract class SqlTransactionGrain<K, S, W> : TransactionGrain<K, S, W>, ISqlGrain
        where S : class, IState<K>, ITransactionable<S>, new()
        where W : IMessageWrapper, new()
    {
        public abstract SqlGrainConfig GrainConfig { get; }
    }
}
