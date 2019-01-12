namespace Ray.Core.State
{
    public interface IStateArchive<K, S>
        where S : IActorState<K>
    {
        S State { get; set; }
        ArchiveInfo ArchiveInfo { get; set; }
    }
}
