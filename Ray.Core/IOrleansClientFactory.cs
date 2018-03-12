using Orleans;

namespace Ray.Core
{
    public interface IOrleansClientFactory
    {
        IClusterClient GetClient();
    }
}
