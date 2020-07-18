using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ray.Core.Channels;
using Ray.Core.Serialization;
using Ray.DistributedTx;
using TransactionStatus = Ray.DistributedTx.TransactionStatus;

namespace Ray.Storage.MySQL
{
    public class DistributedTxStorage : IDistributedTxStorage
    {
        private readonly IMpscChannel<AskInputBox<AppendInput, bool>> mpscChannel;
        private readonly ISerializer serializer;
        private readonly string connection;
        private readonly IOptions<TransactionOptions> options;
        private readonly string deleteSql;
        private readonly string selectListSql;
        private readonly string updateSql;
        private readonly string insertSql;

        public DistributedTxStorage(
            IServiceProvider serviceProvider,
            IOptions<TransactionOptions> options,
            IOptions<MySQLConnections> connectionsOptions)
        {
            this.options = options;
            this.connection = connectionsOptions.Value.ConnectionDict[options.Value.ConnectionKey];
            this.CreateEventSubRecordTable().GetAwaiter().GetResult();
            this.mpscChannel = serviceProvider.GetService<IMpscChannel<AskInputBox<AppendInput, bool>>>();
            this.serializer = serviceProvider.GetService<ISerializer>();
            this.mpscChannel.BindConsumer(this.BatchInsertExecuter);
            this.deleteSql = $"delete from {options.Value.TableName} WHERE UnitName=@UnitName and TransactionId=@TransactionId";
            this.selectListSql = $"select * from {options.Value.TableName} WHERE UnitName=@UnitName";
            this.updateSql = $"update {options.Value.TableName} set Status=@Status where UnitName=@UnitName and TransactionId=@TransactionId";
            this.insertSql = $"INSERT IGNORE INTO {options.Value.TableName}(UnitName,TransactionId,Data,Status,Timestamp) VALUES(@UnitName,@TransactionId,@Data,@Status,@Timestamp)";
        }

        public DbConnection CreateConnection()
        {
            return MySQLFactory.CreateConnection(this.connection);
        }

        public async Task CreateEventSubRecordTable()
        {
            var sql = $@"
                CREATE TABLE if not exists `{this.options.Value.TableName}`(
                     `UnitName` varchar(500) not null,
                     `TransactionId` varchar(500) not null,
                     `Data` json not null,
                     `Status` int2 not null,
                     `Timestamp` int8 not null,
                     UNIQUE INDEX `UnitName_TransId`(`UnitName`, `TransactionId`) USING BTREE);";
            using var connection = this.CreateConnection();
            await connection.ExecuteAsync(sql);
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

        public async Task Delete(string unitName, string transactionId)
        {
            using var conn = this.CreateConnection();
            await conn.ExecuteAsync(this.deleteSql, new { UnitName = unitName, TransactionId = transactionId });
        }

        public async Task<IList<Commit<Input>>> GetList<Input>(string unitName)
            where Input : class, new()
        {
            using var conn = this.CreateConnection();
            return (await conn.QueryAsync<CommitModel>(this.selectListSql, new
            {
                UnitName = unitName
            })).Select(model => new Commit<Input>
            {
                TransactionId = model.TransactionId,
                Status = model.Status,
                Data = this.serializer.Deserialize<Input>(model.Data),
                Timestamp = model.Timestamp
            }).AsList();
        }

        public async Task<bool> Update(string unitName, string transactionId, TransactionStatus status)
        {
            using var conn = this.CreateConnection();
            return await conn.ExecuteAsync(this.updateSql, new { UnitName = unitName, TransactionId = transactionId, Status = status }) > 0;
        }

        private async Task BatchInsertExecuter(List<AskInputBox<AppendInput, bool>> wrapperList)
        {
            using var conn = this.CreateConnection();
            await conn.OpenAsync();
            using var trans = conn.BeginTransaction();
            try
            {
                await conn.ExecuteAsync(this.insertSql, wrapperList.Select(wrapper => new
                {
                    wrapper.Value.UnitName,
                    wrapper.Value.TransactionId,
                    wrapper.Value.Data,
                    Status = (short)wrapper.Value.Status,
                    wrapper.Value.Timestamp
                }).ToList(), trans);
                trans.Commit();
                wrapperList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
            }
            catch (Exception e)
            {
                trans.Rollback();
                wrapperList.ForEach(wrap => wrap.TaskSource.TrySetException(e));
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
