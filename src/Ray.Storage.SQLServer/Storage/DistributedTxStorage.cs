using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ray.Core.Channels;
using Ray.Core.Serialization;
using Ray.DistributedTx;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using SqlBulkCopy = Microsoft.Data.SqlClient.SqlBulkCopy;
using SqlConnection = Microsoft.Data.SqlClient.SqlConnection;
using TransactionStatus = Ray.DistributedTx.TransactionStatus;

namespace Ray.Storage.SQLServer
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
            IOptions<SQLServerConnections> connectionsOptions)
        {
            this.options = options;
            connection = connectionsOptions.Value.ConnectionDict[options.Value.ConnectionKey];
            CreateEventSubRecordTable();
            mpscChannel = serviceProvider.GetService<IMpscChannel<AskInputBox<AppendInput, bool>>>();
            serializer = serviceProvider.GetService<ISerializer>();
            mpscChannel.BindConsumer(BatchInsertExecuter);
            delete_sql = $"delete from {options.Value.TableName} WHERE UnitName=@UnitName and TransactionId=@TransactionId";
            select_list_sql = $"select * from {options.Value.TableName} WHERE UnitName=@UnitName";
            update_sql = $"update {options.Value.TableName} set Status=@Status where UnitName=@UnitName and TransactionId=@TransactionId";
            insert_sql = $"INSERT INTO {options.Value.TableName}(UnitName,TransactionId,Data,Status,Timestamp) VALUES(@UnitName,@TransactionId,@Data,@Status,@Timestamp)";
        }
        public DbConnection CreateConnection()
        {
            return SQLServerFactory.CreateConnection(connection);
        }
        public void CreateEventSubRecordTable()
        {
            var sql = $@"
                if not exists(select 1 from sysobjects where id = object_id('{options.Value.TableName}')and type = 'U')
                CREATE TABLE {options.Value.TableName}(
                     UnitName varchar(500) not null,
                     TransactionId varchar(500) not null,
                     Data nvarchar(max) not null,
                     Status int not null,
                     Timestamp bigint NOT NULL,
                     INDEX UnitName_TransId UNIQUE(UnitName, TransactionId)
                    );";
            using var connection = CreateConnection();
            connection.Execute(sql);
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
                    Status = commit.Status,
                    Timestamp = commit.Timestamp
                });
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                    await writeTask;
                await wrap.TaskSource.Task;
            });
        }

        public async Task Delete(string unitName, string transactionId)
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
                Data = serializer.Deserialize<Input>(model.Data),
                Timestamp = model.Timestamp
            }).AsList();
        }

        public async Task<bool> Update(string unitName, string transactionId, TransactionStatus status)
        {
            using var conn = CreateConnection();
            return await conn.ExecuteAsync(update_sql, new { UnitName = unitName, TransactionId = transactionId, Status = status }) > 0;
        }
        private async Task BatchInsertExecuter(List<AskInputBox<AppendInput, bool>> wrapperList)
        {
            using var conn = CreateConnection() as SqlConnection;
            await conn.OpenAsync();
            using var trans = conn.BeginTransaction();
            try
            {
                using var bulkCopy = new SqlBulkCopy(conn, Microsoft.Data.SqlClient.SqlBulkCopyOptions.UseInternalTransaction, trans)
                {
                    DestinationTableName = options.Value.TableName,
                    BatchSize = wrapperList.Count
                };
                using var dt = new DataTable();
                dt.Columns.Add("UnitName", typeof(string));
                dt.Columns.Add("TransactionId", typeof(string));
                dt.Columns.Add("Data", typeof(string));
                dt.Columns.Add("Status", typeof(int));
                dt.Columns.Add("Timestamp", typeof(long));
                foreach (var item in wrapperList)
                {
                    var row = dt.NewRow();
                    row["UnitName"] = item.Value.UnitName;
                    row["TransactionId"] = item.Value.TransactionId;
                    row["Data"] = item.Value.Data;
                    row["Status"] = (int)item.Value.Status;
                    row["Timestamp"] = item.Value.Timestamp;
                    dt.Rows.Add(row);
                }
                await conn.OpenAsync();
                await bulkCopy.WriteToServerAsync(dt);
                trans.Commit();
                wrapperList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
            }
            catch
            {
                trans.Rollback();
                using var insert_trans = conn.BeginTransaction();
                try
                {
                    await conn.ExecuteAsync(insert_sql, wrapperList.Select(wrapper => new
                    {
                        wrapper.Value.UnitName,
                        wrapper.Value.TransactionId,
                        wrapper.Value.Data,
                        Status = (short)wrapper.Value.Status,
                        wrapper.Value.Timestamp
                    }).ToList(), insert_trans);
                    insert_trans.Commit();
                    wrapperList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
                }
                catch (Exception e)
                {
                    insert_trans.Rollback();
                    wrapperList.ForEach(wrap => wrap.TaskSource.TrySetException(e));
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
