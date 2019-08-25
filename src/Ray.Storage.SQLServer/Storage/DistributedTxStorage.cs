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
        readonly IMpscChannel<AsyncInputEvent<AppendInput, bool>> mpscChannel;
        readonly ISerializer serializer;
        readonly string connection;
        readonly IOptions<TransactionOptions> options;
        public DistributedTxStorage(
            IServiceProvider serviceProvider,
            IOptions<TransactionOptions> options,
            IOptions<SQLServerConnections> connectionsOptions)
        {
            this.options = options;
            connection = connectionsOptions.Value.ConnectionDict[options.Value.ConnectionKey];
            CreateEventSubRecordTable();
            mpscChannel = serviceProvider.GetService<IMpscChannel<AsyncInputEvent<AppendInput, bool>>>();
            serializer = serviceProvider.GetService<ISerializer>();
            mpscChannel.BindConsumer(BatchInsertExecuter);
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
                     TransactionId bigint not null,
                     Data nvarchar(max) not null,
                     Status int not null,
                     INDEX UnitName_TransId UNIQUE(UnitName, TransactionId)
                    );";
            using var connection = CreateConnection();
            connection.Execute(sql);
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

        public async Task Delete(string unitName, long transactionId)
        {
            using var conn = CreateConnection();
            var sql = $"delete from {options.Value.TableName} WHERE UnitName=@UnitName and TransactionId=@TransactionId";
            await conn.ExecuteAsync(sql, new { UnitName = unitName, TransactionId = transactionId });
        }
        public async Task<IList<Commit<Input>>> GetList<Input>(string unitName)
        {
            var getListSql = $"select * from {options.Value.TableName} WHERE UnitName=@UnitName";
            using var conn = CreateConnection();
            return (await conn.QueryAsync<CommitModel>(getListSql, new
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
            var sql = $"update {options.Value.TableName} set Status=@Status where UnitName=@UnitName and TransactionId=@TransactionId";
            using var conn = CreateConnection();
            return await conn.ExecuteAsync(sql, new { UnitName = unitName, TransactionId = transactionId, Status = status }) > 0;
        }
        private async Task BatchInsertExecuter(List<AsyncInputEvent<AppendInput, bool>> wrapperList)
        {
            try
            {
                using var conn = CreateConnection() as SqlConnection;
                using var bulkCopy = new SqlBulkCopy(conn)
                {
                    DestinationTableName = options.Value.TableName,
                    BatchSize = wrapperList.Count
                };
                using var dt = new DataTable();
                dt.Columns.Add("UnitName", typeof(string));
                dt.Columns.Add("TransactionId", typeof(long));
                dt.Columns.Add("Data", typeof(string));
                dt.Columns.Add("Status", typeof(int));
                foreach (var item in wrapperList)
                {
                    var row = dt.NewRow();
                    row["UnitName"] = item.Value.UnitName;
                    row["TransactionId"] = item.Value.TransactionId;
                    row["Data"] = item.Value.Data;
                    row["Status"] = (int)item.Value.Status;
                    dt.Rows.Add(row);
                }
                await conn.OpenAsync();
                await bulkCopy.WriteToServerAsync(dt);
                wrapperList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
            }
            catch
            {
                var saveSql = $"if NOT EXISTS(SELECT * FROM {options.Value.TableName} where UnitName=@UnitName and TransactionId=@TransactionId)INSERT INTO {options.Value.TableName}(UnitName,TransactionId,Data,Status) VALUES(@UnitName,@TransactionId,@Data,@Status)";
                using var conn = CreateConnection();
                await conn.OpenAsync();
                using var trans = conn.BeginTransaction();
                try
                {
                    await conn.ExecuteAsync(saveSql, wrapperList.Select(wrapper => new
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
