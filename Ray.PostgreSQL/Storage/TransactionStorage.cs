using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Ray.Core.Channels;
using Ray.Core.Serialization;
using Ray.DistributedTransaction;
using TransactionStatus = Ray.DistributedTransaction.TransactionStatus;

namespace Ray.Storage.PostgreSQL
{
    public class TransactionStorage : ITransactionStorage
    {
        readonly IMpscChannel<DataAsyncWrapper<AppendInput, bool>> mpscChannel;
        readonly ILogger<TransactionStorage> logger;
        readonly ISerializer serializer;
        readonly string connection;
        readonly IOptions<TransactionOptions> options;
        public TransactionStorage(
            IServiceProvider serviceProvider,
            IOptions<TransactionOptions> options,
            IOptions<PSQLConnections> connectionsOptions)
        {
            this.options = options;
            connection = connectionsOptions.Value.ConnectionDict[options.Value.ConnectionKey];
            CreateEventSubRecordTable().GetAwaiter().GetResult();
            mpscChannel = serviceProvider.GetService<IMpscChannel<DataAsyncWrapper<AppendInput, bool>>>();
            serializer = serviceProvider.GetService<ISerializer>();
            mpscChannel.BindConsumer(BatchProcessing);
            mpscChannel.ActiveConsumer();
        }
        public DbConnection CreateConnection()
        {
            return PSQLFactory.CreateConnection(connection);
        }
        public async Task CreateEventSubRecordTable()
        {
            var sql = $@"
CREATE TABLE if not exists {options.Value.TableName}(
                     UnitName varchar(500) not null,
                     TransactionId int8 not null,
                     Data jsonb not null,
                     Status int2 not null)WITH (OIDS=FALSE);
                     CREATE UNIQUE INDEX IF NOT EXISTS UnitName_TransId ON {options.Value.TableName} USING btree(UnitName, TransactionId)";
            using (var connection = CreateConnection())
            {
                await connection.ExecuteAsync(sql);
            }
        }
        public Task<bool> Append<Input>(string unitName, Commit<Input> commit)
        {
            return Task.Run(async () =>
            {
                var wrap = new DataAsyncWrapper<AppendInput, bool>(new AppendInput
                {
                    UnitName = unitName,
                    TransactionId = commit.TransactionId,
                    Data = serializer.SerializeToString(commit.Data),
                    Status = commit.Status
                });
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                    await writeTask;
                return await wrap.TaskSource.Task;
            });
        }

        public async Task Delete(string unitName, long transactionId)
        {
            using (var conn = CreateConnection())
            {
                var sql = $"delete from {options.Value.TableName} WHERE UnitName=@UnitName and TransactionId=@TransactionId";
                await conn.ExecuteAsync(sql, new { UnitName = unitName, TransactionId = transactionId });
            }
        }
        public async Task<IList<Commit<Input>>> GetList<Input>(string unitName)
        {
            var getListSql = $"select * from {options.Value.TableName} WHERE UnitName=@UnitName";
            using (var conn = CreateConnection())
            {
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
        }

        public async Task<bool> Update(string unitName, long transactionId, TransactionStatus status)
        {
            var sql = $"update {options.Value.TableName} set Status=@Status where UnitName=@UnitName and TransactionId=@TransactionId";
            using (var conn = CreateConnection())
            {
                return await conn.ExecuteAsync(sql, new { UnitName = unitName, TransactionId = transactionId, Status = status }) > 0;
            }
        }
        private async Task BatchProcessing(List<DataAsyncWrapper<AppendInput, bool>> wrapperList)
        {
            var copySql = $"copy {options.Value.TableName}(UnitName,TransactionId,Data,Status) FROM STDIN (FORMAT BINARY)";
            try
            {
                using (var conn = CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    using (var writer = conn.BeginBinaryImport(copySql))
                    {
                        foreach (var wrapper in wrapperList)
                        {
                            writer.StartRow();
                            writer.Write(wrapper.Value.UnitName, NpgsqlDbType.Varchar);
                            writer.Write(wrapper.Value.TransactionId, NpgsqlDbType.Bigint);
                            writer.Write(wrapper.Value.Data, NpgsqlDbType.Jsonb);
                            writer.Write((short)wrapper.Value.Status, NpgsqlDbType.Smallint);
                        }
                        writer.Complete();
                    }
                }
                wrapperList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
            }
            catch
            {
                var saveSql = $"INSERT INTO {options.Value.TableName}(UnitName,TransactionId,Data,Status) VALUES(@UnitName,@TransactionId,(@Data)::jsonb,@Status) ON CONFLICT ON CONSTRAINT TransactionCommit_uniqueue DO NOTHING";
                using (var conn = CreateConnection())
                {
                    await conn.OpenAsync();
                    using (var trans = conn.BeginTransaction())
                    {
                        try
                        {
                            foreach (var wrapper in wrapperList)
                            {
                                wrapper.Value.ReturnValue = await conn.ExecuteAsync(saveSql, new
                                {
                                    wrapper.Value.UnitName,
                                    wrapper.Value.TransactionId,
                                    wrapper.Value.Data,
                                    Status = (short)wrapper.Value.Status
                                }, trans) > 0;
                            }
                            trans.Commit();
                            wrapperList.ForEach(wrap => wrap.TaskSource.TrySetResult(wrap.Value.ReturnValue));
                        }
                        catch (Exception e)
                        {
                            trans.Rollback();
                            wrapperList.ForEach(wrap => wrap.TaskSource.TrySetException(e));
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
        public bool ReturnValue { get; set; }
    }
}
