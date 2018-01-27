using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Ray.Core;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Ray.MongoES
{
    public class MongoStorage
    {
        public static void Init(IServiceProvider provider)
        {
            _Client = new MongoClient(provider.GetService<IOptions<MongoConfig>>().Value.Connection);
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
