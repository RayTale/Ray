using Orleans;

namespace Ray.Core.Client
{
    public interface IClientFactory
    {
        IClusterClient Create();
    }
}
