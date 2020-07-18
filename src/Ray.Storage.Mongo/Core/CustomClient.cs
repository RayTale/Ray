using MongoDB.Driver;

namespace Ray.Storage.Mongo
{
    public class CustomClient : ICustomClient
    {
        public CustomClient(string connection)
        {
            this.Client = new MongoClient(connection);
        }

        public MongoClient Client { get; }

        public IMongoDatabase GetDatabase(string name)
        {
            return this.Client.GetDatabase(name);
        }

        public IMongoCollection<T> GetCollection<T>(string databaseName, string collectionName)
        {
            return this.Client.GetDatabase(databaseName).GetCollection<T>(collectionName);
        }
    }
}
