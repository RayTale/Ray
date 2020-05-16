using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ray.Core.Channels;
using Ray.Core.Serialization;
using Ray.DistributedTx;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using TransactionStatus = Ray.DistributedTx.TransactionStatus;

namespace Ray.Storage.MySQL
{
    public class DistributedTxStorage : IDistributedTxStorage
    {
        readonly IMpscChannel<AskInputBox<AppendInput, bool>> mpscChannel;
        readonly ISerializer serializer;
        readonly string connection;
        readonly IOptions<TransactionOptions> options;
        readonly string delete_sql;
        readonly string select_list_sql;
        readonly string update_sql;
        readonly string insert_sql;
        public DistributedTxStorage(
            IServiceProvider serviceProvider,
            IOptions<TransactionOptions> options,
            IOptions<MySQLConnections> connectionsOptions)
        {
            this.options = options;
            connection = connectionsOptions.Value.ConnectionDict[options.Value.ConnectionKey];
            CreateEventSubRecordTable().GetAwaiter().GetResult();
            mpscChannel = serviceProvider.GetService<IMpscChannel<AskInputBox<AppendInput, bool>>>();
            serializer = serviceProvider.GetService<ISerializer>();
            mpscChannel.BindConsumer(BatchInsertExecuter);
            delete_sql= $"delete from {options.Value.TableName} WHERE UnitName=@UnitName and TransactionId=@TransactionId"; 
            select_list_sql = $"select * from {options.Value.TableName} WHERE UnitName=@UnitName";
            update_sql = $"update {options.Value.TableName} set Status=@Status where UnitName=@UnitName and TransactionId=@TransactionId";
            insert_sql = $"INSERT IGNORE INTO {options.Value.TableName}(UnitName,TransactionId,Data,Status) VALUES(@UnitName,@TransactionId,@Data,@Status)";
        }
        public DbConnection CreateConnection()
        {
            return MySQLFactory.CreateConnection(connection);
        }
        public async Task CreateEventSubRecordTable()
        {
            var sql = $@"
                CREATE TABLE if not exists `{options.Value.TableName}`(
                     `UnitName` varchar(500) not null,
                     `TransactionId` int8 not null,
                     `Data` json not null,
                     `Status` int2 not null,
                     UNIQUE INDEX `UnitName_TransId`(`UnitName`, `TransactionId`) USING BTREE);";
            using var connection = CreateConnection();
            await connection.ExecuteAsync(sql);
        }
        public Task Append<Input>(string unitName, Commit<Input> commit) where Input : class, new()
        {
            return Task.Run(async () =>
            {
                var wrap = new AskInputBox<AppendInput, bool>(new AppendInput
                {
                    UnitName = unitName,
                    TransactionId = commit.TransactionId,
                    Data = serializer.Serialize(commit.Data),
                    Status = commit.Status
                });
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                    await writeTask;
                await wrap.TaskSource.Task;
            });
        }

        public async Task Delete(string unitName, long transactionId)
        {
            using var conn = CreateConnection();
            await conn.ExecuteAsync(delete_sql, new { UnitName = unitName, TransactionId = transactionId });
        }
        public async Task<IList<Commit<Input>>> GetList<Input>(string unitName) where Input : class, new()
        {
            using var conn = CreateConnection();
            return (await conn.QueryAsync<CommitModel>(select_list_sql, new
            {
                UnitName = unitName
            })).Select(model => new Commit<Input>
            {
                TransactionId = model.TransactionId,
                Status = model.Status,
                Data = serializer.Deserialize<Input>(model.Data)
            }).AsList();
        }

        public async Task<bool> Update(string unitName, long transactionId, TransactionStatus status)
        {
            using var conn = CreateConnection();
            return await conn.ExecuteAsync(update_sql, new { UnitName = unitName, TransactionId = transactionId, Status = status }) > 0;
        }
        private async Task BatchInsertExecuter(List<AskInputBox<AppendInput, bool>> wrapperList)
        {
            using var conn = CreateConnection();
            await conn.OpenAsync();
            using var trans = conn.BeginTransaction();
            try
            {
                await conn.ExecuteAsync(insert_sql, wrapperList.Select(wrapper => new
                {
                    wrapper.Value.UnitName,
                    wrapper.Value.TransactionId,
                    wrapper.Value.Data,
                    Status = (short)wrapper.Value.Status
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
        public long TransactionId { get; set; }
        public string Data { get; set; }
        public TransactionStatus Status { get; set; }
    }
    public class AppendInput : CommitModel
    {
        public string UnitName { get; set; }
    }
}
