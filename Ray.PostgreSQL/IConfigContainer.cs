using System;
using Orleans;

namespace Ray.Storage.PostgreSQL
{
    public interface IConfigContainer
    {
        GrainConfigBuilder<K> CreateBuilder<K>(Func<Grain, K, SqlGrainConfig> generator, bool ignoreGrainId = true);
        void RegisterBuilder<K>(Type type, GrainConfigBuilderWrapper<K> builder);
    }
}
