using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Ray.MongoES
{
    public class MongoStorage
    {
        static object lockObj = new object();
        public MongoStorage(IOptions<MongoConfig> config)
        {
            if (_Client == null)
            {
                lock (lockObj)
                {
                    if (_Client == null)
                    {
                        _Client = new MongoClient(config.Value.Connection);
                    }
                }
            }
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
