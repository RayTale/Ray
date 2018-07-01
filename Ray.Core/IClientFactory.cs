using Orleans;

namespace Ray.Core
{
    public interface IClientFactory
    {
        IClusterClient GetClient();
    }
}
