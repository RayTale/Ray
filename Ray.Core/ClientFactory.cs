using Orleans;
using System;
using System.Threading.Tasks;

namespace Ray.Core
{
    public class ClientFactory : IClientFactory
    {
        static Func<IClientBuilder> _builderFunc;
        static IClusterClient _client;
        static bool needReBuild = false;
        public static async Task<IClusterClient> Build(Func<IClientBuilder> builderFunc)
        {
            _builderFunc = builderFunc;
            _client = builderFunc().Build();
            await _client.Connect();
            return _client;
        }
        public static void ReBuild()
        {
            if (_client != null)
            {
                needReBuild = true;
            }
        }
        readonly object connectLock = new object();
        public IClusterClient GetClient()
        {
            if (!_client.IsInitialized || needReBuild)
            {
                lock (connectLock)
                {
                    if (!_client.IsInitialized || needReBuild)
                    {
                        if (needReBuild)
                        {
                            _client.Close();
                            _client.Dispose();
                        }
                        _client = _builderFunc().Build();
                        _client.Connect().GetAwaiter().GetResult();
                        needReBuild = false;
                    }
                }
            }
            return _client;
        }
    }
}
