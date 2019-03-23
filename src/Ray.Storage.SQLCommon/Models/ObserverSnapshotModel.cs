namespace Ray.Storage.SQLCore
{
    public class ObserverSnapshotModel
    {
        public string StateId { get; set; }
        public long Version { get; set; }
        public long StartTimestamp { get; set; }
    }
}
