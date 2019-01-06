using System;
using System.Collections.Generic;
using Orleans;

namespace Ray.Storage.MongoDB
{
    public class GrainConfigBuilder<K>
    {
        readonly Func<Grain, K, MongoGrainConfig> generator;
        readonly IConfigContainer configContainer;
        readonly List<(Type type, string snapshotCollection)> grains = new List<(Type type, string snapshotCollection)>();
        readonly bool ignoreGrainId;
        public GrainConfigBuilder(
            IConfigContainer configContainer,
            Func<Grain, K, MongoGrainConfig> generator,
            bool ignoreGrainId)
        {
            this.configContainer = configContainer;
            this.generator = generator;
            this.ignoreGrainId = ignoreGrainId;
        }
        public GrainConfigBuilder<K> BindToGrain<T>(string snapshotCollection = null)
        {
            grains.Add((typeof(T), snapshotCollection));
            return this;
        }
        public void Enable()
        {
            foreach (var (type, snapshotCollection) in grains)
            {
                configContainer.RegisterBuilder(type, new GrainConfigBuilderWrapper<K>
                {
                    IgnoreGrainId = ignoreGrainId,
                    SnapshotCollection = snapshotCollection,
                    Generator = generator
                });
            }
        }
    }
}
