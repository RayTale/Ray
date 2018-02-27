using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Ray.MongoES
{
    public interface IMongoStorage
    {
        MongoConfig Config { get; }
        MongoClient Client { get; }
        IMongoDatabase GetDatabase(string name);
        IMongoCollection<T> GetCollection<T>(string databaseName, string collectionName);
    }
    public class MongoStorage : IMongoStorage
    {
        MongoConfig config;
        public MongoStorage(IOptions<MongoConfig> config)
        {
            this.config = config.Value;
            _Client = new MongoClient(config.Value.Connection);
        }
        public MongoConfig Config { get { return config; } }
        MongoClient _Client;
        public MongoClient Client
        {
            get
            {
                return _Client;
            }
        }
        public IMongoDatabase GetDatabase(string name)
        {
            return Client.GetDatabase(name);
        }
        public IMongoCollection<T> GetCollection<T>(string databaseName, string collectionName)
        {
            return Client.GetDatabase(databaseName).GetCollection<T>(collectionName);
        }
    }
}
