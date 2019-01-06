using System;
using Orleans;

namespace Ray.Storage.MongoDB
{
    public class GrainConfigBuilderWrapper<K>
    {
        public bool IgnoreGrainId { get; set; }
        public string SnapshotCollection { get; set; }
        public Func<Grain, K, MongoGrainConfig> Generator { get; set; }
    }
}
