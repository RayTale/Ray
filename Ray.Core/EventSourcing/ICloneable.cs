namespace Ray.Core.EventSourcing
{
    public interface ITransactionable<T>
    {
        T DeepCopy();
    }
}
