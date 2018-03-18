using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Ray.MongoDb
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
        public MongoStorage(IOptions<MongoConfig> config)
        {
            Config = config.Value;
            Client = new MongoClient(config.Value.Connection);
        }
        public MongoConfig Config { get; }
        public MongoClient Client { get; }
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
