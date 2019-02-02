namespace Ray.Core.State
{
    public class Snapshot<K, S>
        where S : class, new()
    {
        public Snapshot()
        {
        }
        public Snapshot(K stateId)
        {
            Base = new SnapshotBase<K>
            {
                StateId = stateId
            };
            State = new S();
        }
        public SnapshotBase<K> Base { get; set; }
        public S State { get; set; }
    }
}
