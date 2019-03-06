using MongoDB.Driver;

namespace Ray.Storage.MongoDB
{
    public interface ICustomClient
    {
        MongoClient Client { get; }
        IMongoDatabase GetDatabase(string name);
        IMongoCollection<T> GetCollection<T>(string databaseName, string collectionName);
    }
}
