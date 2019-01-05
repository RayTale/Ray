using Orleans;

namespace Ray.Core.Client
{
    public interface IClusterClientFactory
    {
        IClusterClient Create();
    }
}
