using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Channels;
using Ray.Core.Serialization;
using Ray.DistributedTransaction;
using Ray.Storage.Mongo.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.Storage.Mongo.Storage
{
    public class DistributedTxStorage : IDistributedTxStorage
    {
        readonly IMpscChannel<AsyncInputEvent<AppendInput, bool>> mpscChannel;
        readonly ISerializer serializer;
        readonly ICustomClient client;
        readonly IOptions<TransactionOptions> transactionOptions;
        public DistributedTxStorage(
            IServiceProvider serviceProvider,
            IOptions<TransactionOptions> transactionOptions,
            IOptions<MongoConnections> connectionsOptions)
        {
            this.transactionOptions = transactionOptions;
            client = ClientFactory.CreateClient(connectionsOptions.Value.ConnectionDict[transactionOptions.Value.ConnectionKey]);
            mpscChannel = serviceProvider.GetService<IMpscChannel<AsyncInputEvent<AppendInput, bool>>>();
            serializer = serviceProvider.GetService<ISerializer>();
            serviceProvider.GetService<IIndexBuildService>().CreateTransactionStorageIndex(client, transactionOptions.Value.Database, transactionOptions.Value.CollectionName).GetAwaiter().GetResult();
            mpscChannel.BindConsumer(BatchProcessing);
        }
        public Task Append<Input>(string unitName, Commit<Input> commit)
        {
            return Task.Run(async () =>
            {
                var wrap = new AsyncInputEvent<AppendInput, bool>(new AppendInput
                {
                    UnitName = unitName,
                    TransactionId = commit.TransactionId,
                    Data = serializer.SerializeToString(commit.Data),
                    Status = commit.Status
                });
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                    await writeTask;
                await wrap.TaskSource.Task;
            });
        }

        public Task Delete(string unitName, long transactionId)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("UnitName", unitName) & Builders<BsonDocument>.Filter.Eq("TransactionId", transactionId);
            return client.GetCollection<BsonDocument>(transactionOptions.Value.Database, transactionOptions.Value.CollectionName).DeleteOneAsync(filter);
        }

        public async Task<IList<Commit<Input>>> GetList<Input>(string unitName)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("UnitName", unitName);
            var cursor = await client.GetCollection<BsonDocument>(transactionOptions.Value.Database, transactionOptions.Value.CollectionName).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
            var list = new List<Commit<Input>>();
            foreach (var document in cursor.ToEnumerable())
            {
                list.Add(new Commit<Input>
                {
                    TransactionId = document["TransactionId"].AsInt64,
                    Status = (TransactionStatus)document["Status"].AsInt32,
                    Data = serializer.Deserialize<Input>(document["Data"].AsString)
                });
            }
            return list;
        }

        public async Task<bool> Update(string unitName, long transactionId, TransactionStatus status)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("UnitName", unitName) & Builders<BsonDocument>.Filter.Eq("TransactionId", transactionId);
            var update = Builders<BsonDocument>.Update.Set("Status", (short)status);
            await client.GetCollection<BsonDocument>(transactionOptions.Value.Database, transactionOptions.Value.CollectionName).UpdateOneAsync(filter, update, null, new CancellationTokenSource(3000).Token);
            return true;
        }
        private async Task BatchProcessing(List<AsyncInputEvent<AppendInput, bool>> wrapperList)
        {
            var collection = client.GetCollection<BsonDocument>(transactionOptions.Value.Database, transactionOptions.Value.CollectionName);
            var documents = wrapperList.Select(wrapper => (wrapper, new BsonDocument
                {
                    {"UnitName",BsonValue.Create( wrapper.Value.UnitName) },
                    {"TransactionId",wrapper.Value.TransactionId },
                    {"Data",wrapper.Value.Data},
                    {"Status",(int)wrapper.Value.Status }
                }));
            var session = await client.Client.StartSessionAsync();
            session.StartTransaction(new MongoDB.Driver.TransactionOptions(readConcern: ReadConcern.Snapshot, writeConcern: WriteConcern.WMajority));
            try
            {
                await collection.InsertManyAsync(session, documents.Select(d => d.Item2));
                await session.CommitTransactionAsync();
                wrapperList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
            }
            catch
            {
                await session.AbortTransactionAsync();
                foreach (var document in documents)
                {
                    try
                    {
                        await collection.InsertOneAsync(document.Item2);
                        document.wrapper.TaskSource.TrySetResult(true);
                    }
                    catch (MongoWriteException ex)
                    {
                        if (ex.WriteError.Category != ServerErrorCategory.DuplicateKey)
                        {
                            document.wrapper.TaskSource.TrySetException(ex);
                        }
                        else
                        {
                            document.wrapper.TaskSource.TrySetResult(false);
                        }
                    }
                }
            }
        }
    }
    public class CommitModel
    {
        public long TransactionId { get; set; }
        public string Data { get; set; }
        public TransactionStatus Status { get; set; }
    }
    public class AppendInput : CommitModel
    {
        public string UnitName { get; set; }
    }
}
