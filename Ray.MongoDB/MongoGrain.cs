using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.MongoDB
{
    public abstract class MongoGrain<K, S, W> : ESGrain<K, S, W>, IMongoGrain
        where S : class, IState<K>, new()
        where W : MessageWrapper
    {
        public abstract MongoGrainConfig GrainConfig { get; }
    }
}
