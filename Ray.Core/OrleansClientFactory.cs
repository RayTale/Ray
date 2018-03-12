using Orleans;

namespace Ray.Core
{
    public class OrleansClientFactory : IOrleansClientFactory
    {
        static IClusterClient _client;
        public static void Init(IClusterClient client)
        {
            _client = client;
        }
        public IClusterClient GetClient()
        {
            return _client;
        }
    }
}
