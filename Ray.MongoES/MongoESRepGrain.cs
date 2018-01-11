using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.MongoES
{
    public abstract class MongoESRepGrain<K, S, W> : ESRepGrain<K, S, W>, IMongoGrain
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
