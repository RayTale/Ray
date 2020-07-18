using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Channels;
using Ray.Core.Serialization;
using Ray.DistributedTx;
using Ray.Storage.Mongo.Core;

namespace Ray.Storage.Mongo.Storage
{
    public class DistributedTxStorage : IDistributedTxStorage
    {
        private readonly IMpscChannel<AskInputBox<AppendInput, bool>> mpscChannel;
        private readonly ISerializer serializer;
        private readonly ICustomClient client;
        private readonly IOptions<TransactionOptions> transactionOptions;

        public DistributedTxStorage(
            IServiceProvider serviceProvider,
            IOptions<TransactionOptions> transactionOptions,
            IOptions<MongoConnections> connectionsOptions)
        {
            this.transactionOptions = transactionOptions;
            this.client = ClientFactory.CreateClient(connectionsOptions.Value.ConnectionDict[transactionOptions.Value.ConnectionKey]);
            this.mpscChannel = serviceProvider.GetService<IMpscChannel<AskInputBox<AppendInput, bool>>>();
            this.serializer = serviceProvider.GetService<ISerializer>();
            serviceProvider.GetService<IIndexBuildService>().CreateTransactionStorageIndex(this.client, transactionOptions.Value.Database, transactionOptions.Value.CollectionName).GetAwaiter().GetResult();
            this.mpscChannel.BindConsumer(this.BatchInsertExecuter);
        }

        public Task Append<Input>(string unitName, Commit<Input> commit)
            where Input : class, new()
        {
            return Task.Run(async () =>
            {
                var wrap = new AskInputBox<AppendInput, bool>(new AppendInput
                {
                    UnitName = unitName,
                    TransactionId = commit.TransactionId,
                    Data = this.serializer.Serialize(commit.Data),
                    Status = commit.Status,
                    Timestamp = commit.Timestamp
                });
                var writeTask = this.mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                {
                    await writeTask;
                }

                await wrap.TaskSource.Task;
            });
        }

        public Task Delete(string unitName, string transactionId)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("UnitName", unitName) & Builders<BsonDocument>.Filter.Eq("TransactionId", transactionId);
            return this.client.GetCollection<BsonDocument>(this.transactionOptions.Value.Database, this.transactionOptions.Value.CollectionName).DeleteOneAsync(filter);
        }

        public async Task<IList<Commit<Input>>> GetList<Input>(string unitName)
            where Input : class, new()
        {
            var filter = Builders<BsonDocument>.Filter.Eq("UnitName", unitName);
            var cursor = await this.client.GetCollection<BsonDocument>(this.transactionOptions.Value.Database, this.transactionOptions.Value.CollectionName).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
            var list = new List<Commit<Input>>();
            foreach (var document in cursor.ToEnumerable())
            {
                list.Add(new Commit<Input>
                {
                    TransactionId = document["TransactionId"].AsString,
                    Status = (TransactionStatus)document["Status"].AsInt32,
                    Data = this.serializer.Deserialize<Input>(document["Data"].AsString),
                    Timestamp = document["Timestamp"].AsInt64
                });
            }

            return list;
        }

        public async Task<bool> Update(string unitName, string transactionId, TransactionStatus status)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("UnitName", unitName) & Builders<BsonDocument>.Filter.Eq("TransactionId", transactionId);
            var update = Builders<BsonDocument>.Update.Set("Status", (short)status);
            await this.client.GetCollection<BsonDocument>(this.transactionOptions.Value.Database, this.transactionOptions.Value.CollectionName).UpdateOneAsync(filter, update, null, new CancellationTokenSource(3000).Token);
            return true;
        }

        private async Task BatchInsertExecuter(List<AskInputBox<AppendInput, bool>> wrapperList)
        {
            var collection = this.client.GetCollection<BsonDocument>(this.transactionOptions.Value.Database, this.transactionOptions.Value.CollectionName);
            var documents = wrapperList.Select(wrapper => (wrapper, new BsonDocument
                {
                    { "UnitName", BsonValue.Create( wrapper.Value.UnitName) },
                    { "TransactionId", wrapper.Value.TransactionId },
                    { "Data", wrapper.Value.Data },
                    { "Status", (int)wrapper.Value.Status },
                    { "Timestamp", wrapper.Value.Timestamp }
                }));
            var session = await this.client.Client.StartSessionAsync();
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
        public string TransactionId { get; set; }

        public string Data { get; set; }

        public TransactionStatus Status { get; set; }

        public long Timestamp { get; set; }
    }

    public class AppendInput : CommitModel
    {
        public string UnitName { get; set; }
    }
}
