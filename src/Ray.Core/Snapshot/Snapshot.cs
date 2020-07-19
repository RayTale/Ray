namespace Ray.Core.Snapshot
{
    public class Snapshot<PrimaryKey, StateType>
        where StateType : class, new()
    {
        public Snapshot()
        {
        }

        public Snapshot(PrimaryKey stateId)
        {
            this.Base = new SnapshotBase<PrimaryKey>
            {
                StateId = stateId
            };
            this.State = new StateType();
        }

        public SnapshotBase<PrimaryKey> Base { get; set; }

        public StateType State { get; set; }
    }
}
