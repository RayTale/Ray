namespace Ray.Core.Storage
{
    public interface IConfigureBuilder<K, C, P>
    {
        IConfigureBuilder<K, C, P> BindTo<T>(P parameter);
        void Complete(IConfigureBuilderContainer container = default);
    }
}
