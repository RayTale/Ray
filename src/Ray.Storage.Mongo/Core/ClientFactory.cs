using System.Collections.Concurrent;

namespace Ray.Storage.Mongo.Core
{
    public static class ClientFactory
    {
        private static readonly ConcurrentDictionary<string, ICustomClient> clientDict = new ConcurrentDictionary<string, ICustomClient>();

        public static ICustomClient CreateClient(string connection)
        {
            return clientDict.GetOrAdd(connection, key => new CustomClient(key));
        }
    }
}
