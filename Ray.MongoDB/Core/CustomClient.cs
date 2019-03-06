using MongoDB.Driver;

namespace Ray.Storage.MongoDB
{
    public class CustomClient : ICustomClient
    {
        public CustomClient(string connection)
        {
            Client = new MongoClient(connection);
        }
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
