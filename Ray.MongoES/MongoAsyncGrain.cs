using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.MongoDb
{
    public abstract class MongoAsyncGrain<K, S, W> : AsyncGrain<K, S, W>, IMongoGrain
        where S : class, IState<K>, new()
        where W : MessageWrapper
    {
        protected MongoStorageAttribute _mongoInfo = null;
        public virtual MongoStorageAttribute ESMongoInfo
        {
            get
            {
                return _mongoInfo;
            }
        }
    }
}
