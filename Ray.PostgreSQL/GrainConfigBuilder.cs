using System;
using System.Collections.Generic;
using Orleans;

namespace Ray.Storage.PostgreSQL
{
    public class GrainConfigBuilder<K>
    {
        readonly Func<Grain, K, SqlGrainConfig> generator;
        readonly IConfigContainer configContainer;
        readonly List<(Type type, string snapshotTable)> grains = new List<(Type type, string snapshotTable)>();
        readonly bool ignoreGrainId;
        public GrainConfigBuilder(
            IConfigContainer configContainer,
            Func<Grain, K, SqlGrainConfig> generator,
            bool ignoreGrainId)
        {
            this.configContainer = configContainer;
            this.generator = generator;
            this.ignoreGrainId = ignoreGrainId;
        }
        public GrainConfigBuilder<K> BindToGrain<T>(string snapshotTable = null)
        {
            grains.Add((typeof(T), snapshotTable));
            return this;
        }
        public void Enable()
        {
            foreach (var (type, snapshotTable) in grains)
            {
                configContainer.RegisterBuilder(type, new GrainConfigBuilderWrapper<K>
                {
                    IgnoreGrainId = ignoreGrainId,
                    SnapshotTable = snapshotTable,
                    Generator = generator
                });
            }
        }
    }
}
