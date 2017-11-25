using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Ray.Core;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.MongoES
{
    public class MongoStorage
    {
        static MongoStorage()
        {
            _Client = new MongoClient(Global.IocProvider.GetService<IOptions<MongoConfig>>().Value.Connection);
        }
        static MongoClient _Client;
        protected static MongoClient Client
        {
            get
            {
                return _Client;
            }
        }
        public static IMongoDatabase GetDatabase(string name)
        {
            return Client.GetDatabase(name);
        }
        public static IMongoCollection<T> GetCollection<T>(string databaseName, string collectionName)
        {
            return Client.GetDatabase(databaseName).GetCollection<T>(collectionName);
        }
    }
}
