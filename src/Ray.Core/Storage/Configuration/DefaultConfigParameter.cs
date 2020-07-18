namespace Ray.Core.Storage
{
    public class DefaultConfigParameter : IConfigParameter
    {
        public DefaultConfigParameter(bool singleton)
        {
            this.Singleton = singleton;
        }

        public bool Singleton { get; set; }
    }
}
