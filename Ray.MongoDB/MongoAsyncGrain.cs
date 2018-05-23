using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.MongoDB
{
    public abstract class MongoAsyncGrain<K, S, W> : AsyncGrain<K, S, W>, IMongoGrain
        where S : class, IState<K>, new()
        where W : IMessageWrapper
    {
        protected MongoGrainConfig _mongoInfo = null;
        public virtual MongoGrainConfig GrainConfig
        {
            get
            {
                return _mongoInfo;
            }
        }
    }
}
