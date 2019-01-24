namespace Ray.Core.State
{
    public class SnapshotArchive<K, S, B>
        where S : IState<K, B>
        where B : ISnapshot<K>, new()
    {
        public S State { get; set; }
        public ArchiveBrief ArchiveInfo { get; set; }
    }
}
