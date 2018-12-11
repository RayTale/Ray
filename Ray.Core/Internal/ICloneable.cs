namespace Ray.Core.Internal
{
    public interface ITransactionable<T>
    {
        T DeepCopy();
    }
}
