namespace Ray.Core.Storage
{
    public class DefaultConfigParameter : IConfigParameter
    {
        public DefaultConfigParameter(bool singleton)
        {
            Singleton = singleton;
        }
        public bool Singleton { get; set; }
    }
}
