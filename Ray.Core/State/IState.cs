namespace Ray.Core.State
{
    public interface IState<K, B>
        where B : IStateBase<K>, new()
    {
        B Base { get; set; }
    }
}
