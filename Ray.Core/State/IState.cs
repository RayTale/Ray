namespace Ray.Core.State
{
    public interface IState<K>
    {
        IStateBase<K> Base { get; set; }
    }
}
