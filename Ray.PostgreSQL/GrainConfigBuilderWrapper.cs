using System;
using Orleans;

namespace Ray.Storage.PostgreSQL
{
    public class GrainConfigBuilderWrapper<K>
    {
        public bool IgnoreGrainId { get; set; }
        public string SnapshotTable { get; set; }
        public Func<Grain, K, SqlGrainConfig> Generator { get; set; }
    }
}
