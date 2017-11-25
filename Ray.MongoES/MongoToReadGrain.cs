using System;
using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.MongoES
{
    public abstract class MongoToReadGrain<K, W> : ToReadGrain<K, ToReadState<K>, W>, IMongoGrain
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
