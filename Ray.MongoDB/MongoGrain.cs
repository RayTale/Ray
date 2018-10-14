using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.MongoDB
{
    public abstract class MongoGrain<K, S, W> : ESGrain<K, S, W>, IMongoGrain
        where S : class, IState<K>, new()
        where W : IMessageWrapper, new()
    {
        public abstract MongoGrainConfig GrainConfig { get; }
    }
    public abstract class MongoTransactionGrain<K, S, W> : TransactionGrain<K, S, W>, IMongoGrain
        where S : class, IState<K>, ITransactionable<S>, new()
        where W : IMessageWrapper, new()
    {
        public abstract MongoGrainConfig GrainConfig { get; }
    }
}
