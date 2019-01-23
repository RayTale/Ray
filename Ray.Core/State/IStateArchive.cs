namespace Ray.Core.State
{
    public interface IStateArchive<K, S, B>
        where S : IState<K, B>
        where B : IStateBase<K>, new()
    {
        S State { get; set; }
        BriefArchive ArchiveInfo { get; set; }
    }
}
