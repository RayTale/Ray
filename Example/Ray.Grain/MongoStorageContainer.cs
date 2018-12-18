using Ray.Core.Internal;
using Ray.Grain;
using Ray.MongoDB;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Ray.Grain
{
    public class MongoStorageContainer : IStorageContainer
    {
        readonly IMongoStorage mongoStorage;
        readonly IServiceProvider serviceProvider;
        public MongoStorageContainer(IServiceProvider serviceProvider, IMongoStorage mongoStorage)
        {
            this.mongoStorage = mongoStorage;
            this.serviceProvider = serviceProvider;
        }
        readonly ConcurrentDictionary<string, MongoGrainConfig> sqlGrainConfigDict = new ConcurrentDictionary<string, MongoGrainConfig>();
        private async ValueTask<MongoGrainConfig> GetConfig(Orleans.Grain grain)
        {
            var spilitTableStartTime = DateTime.UtcNow;
            MongoGrainConfig result;
            switch (grain)
            {
                case Account value: result = sqlGrainConfigDict.GetOrAdd(value.GetType().FullName, key => new MongoGrainConfig(mongoStorage, "Ray", "account_event", "account_state", spilitTableStartTime)); break;
                case AccountRep value: result = sqlGrainConfigDict.GetOrAdd(value.GetType().FullName, key => new MongoGrainConfig(mongoStorage, "Ray", "account_event", "account_state", spilitTableStartTime)); break;
                case AccountDb value: result = sqlGrainConfigDict.GetOrAdd(value.GetType().FullName, key => new MongoGrainConfig(mongoStorage, "Ray", "account_event", "account_db_state", spilitTableStartTime)); break;
                case AccountFlow value: result = sqlGrainConfigDict.GetOrAdd(value.GetType().FullName, key => new MongoGrainConfig(mongoStorage, "Ray", "account_event", "account_flow_state", spilitTableStartTime)); ; break;
                default: throw new NotImplementedException(nameof(GetEventStorage));
            }
            var buildTask = result.Build();
            if (!buildTask.IsCompleted)
                await buildTask;
            return result;
        }
        readonly ConcurrentDictionary<string, object> eventStorageDict = new ConcurrentDictionary<string, object>();
        public ValueTask<IEventStorage<K>> GetEventStorage<K, S>(Orleans.Grain grain) where S : class, IState<K>, new()
        {
            switch (grain)
            {
                case Account value: return GetFromType(value.GetType());
                case AccountDb value: return GetFromType(value.GetType());
                case AccountFlow value: return GetFromType(value.GetType());
                case AccountRep value: return GetFromType(typeof(Account));
                default: throw new NotImplementedException(nameof(GetEventStorage));
            }
            //通过类型获取Storage
            async ValueTask<IEventStorage<K>> GetFromType(Type type)
            {
                if (!eventStorageDict.TryGetValue(type.FullName, out var storage))
                {
                    var grainConfigTask = GetConfig(grain);
                    if (!grainConfigTask.IsCompleted)
                        await grainConfigTask;
                    storage = eventStorageDict.GetOrAdd(type.FullName, key =>
                    {
                        return new MongoEventStorage<K>(serviceProvider, grainConfigTask.Result);
                    });
                }
                return storage as MongoEventStorage<K>;
            }
        }
        readonly ConcurrentDictionary<string, object> stateStorageDict = new ConcurrentDictionary<string, object>();
        public ValueTask<IStateStorage<S, K>> GetStateStorage<K, S>(Orleans.Grain grain) where S : class, IState<K>, new()
        {
            switch (grain)
            {
                case Account value: return GetFromType(value.GetType());
                case AccountDb value: return GetFromType(value.GetType());
                case AccountFlow value: return GetFromType(value.GetType());
                case AccountRep value: return GetFromType(typeof(Account));
                default: throw new NotImplementedException(nameof(GetEventStorage));
            }
            //通过类型获取Storage
            async ValueTask<IStateStorage<S, K>> GetFromType(Type type)
            {
                if (!stateStorageDict.TryGetValue(type.FullName, out var storage))
                {
                    var grainConfigTask = GetConfig(grain);
                    if (!grainConfigTask.IsCompleted)
                        await grainConfigTask;
                    storage = stateStorageDict.GetOrAdd(type.FullName, key =>
                    {
                        return new MongoStateStorage<S, K>(grainConfigTask.Result);
                    });
                }
                return storage as MongoStateStorage<S, K>;
            }
        }
    }
}
