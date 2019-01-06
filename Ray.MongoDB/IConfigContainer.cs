using System;
using Orleans;

namespace Ray.Storage.MongoDB
{
    public interface IConfigContainer
    {
        GrainConfigBuilder<K> CreateBuilder<K>(Func<Grain, K, MongoGrainConfig> generator, bool ignoreGrainId = true);
        void RegisterBuilder<K>(Type type, GrainConfigBuilderWrapper<K> builder);
    }
}
