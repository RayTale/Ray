using System;
using System.Threading;
using System.Threading.Tasks;
using Ray.Core;
using Ray.Core.EventSourcing;
using Ray.Core.Utils;

namespace Ray.MongoES
{
    public abstract class MongoESGrain<K, S, W> : ESGrain<K, S, W>, IMongoGrain
        where S : class, IState<K>, new()
        where W : MessageWrapper
    {
        protected MongoStorageAttribute _mongoInfo = null;
        public override Task OnActivateAsync()
        {
            return RayTask.Execute(() =>
             {
                 return base.OnActivateAsync();
             });
        }
        public virtual MongoStorageAttribute ESMongoInfo
        {
            get
            {
                return _mongoInfo;
            }
        }
    }
}
