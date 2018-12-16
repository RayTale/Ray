namespace Ray.Core.Internal
{
    public interface ICloneable<T>
    {
        T DeepCopy();
    }
}
