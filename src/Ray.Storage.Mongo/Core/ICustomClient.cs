using MongoDB.Driver;

namespace Ray.Storage.Mongo
{
    public interface ICustomClient
    {
        MongoClient Client { get; }

        IMongoDatabase GetDatabase(string name);

        IMongoCollection<T> GetCollection<T>(string databaseName, string collectionName);
    }
}
