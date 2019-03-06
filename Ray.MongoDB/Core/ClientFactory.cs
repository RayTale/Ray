using System.Collections.Concurrent;

namespace Ray.Storage.MongoDB.Core
{
    public static class ClientFactory
    {
        static readonly ConcurrentDictionary<string, ICustomClient> clientDict = new ConcurrentDictionary<string, ICustomClient>();
        public static ICustomClient CreateClient(string connection)
        {
            return clientDict.GetOrAdd(connection, key => new CustomClient(key));
        }
    }
}
