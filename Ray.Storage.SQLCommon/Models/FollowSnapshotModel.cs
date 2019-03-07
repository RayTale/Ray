namespace Ray.Storage.SQLCore
{
    public class FollowSnapshotModel
    {
        public string StateId { get; set; }
        public long Version { get; set; }
        public long StartTimestamp { get; set; }
    }
}
