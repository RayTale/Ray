namespace Ray.Core.State
{
    public interface IState<K, B>
        where B : ISnapshot<K>, new()
    {
        B Base { get; set; }
    }
}
