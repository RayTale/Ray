using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.MongoES
{
    public abstract class MongoToDbGrain<K, W> : ToDbGrain<K, ToDbState<K>, W>, IMongoGrain
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
