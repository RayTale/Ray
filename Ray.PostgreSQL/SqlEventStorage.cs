using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Serialization;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class SqlEventStorage<PrimaryKey> : IEventStorage<PrimaryKey>
    {
        readonly StorageConfig tableInfo;
        readonly IMpscChannel<DataAsyncWrapper<SaveTransport<PrimaryKey>, bool>> mpscChannel;
        readonly ILogger<SqlEventStorage<PrimaryKey>> logger;
        readonly ISerializer serializer;
        public SqlEventStorage(IServiceProvider serviceProvider, StorageConfig tableInfo)
        {
            logger = serviceProvider.GetService<ILogger<SqlEventStorage<PrimaryKey>>>();
            serializer = serviceProvider.GetService<ISerializer>();
            mpscChannel = serviceProvider.GetService<IMpscChannel<DataAsyncWrapper<SaveTransport<PrimaryKey>, bool>>>().BindConsumer(BatchProcessing);
            mpscChannel.ActiveConsumer();
            this.tableInfo = tableInfo;
        }
        public async Task<IList<IFullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion)
        {
            var originList = new List<EventBytesWrapper>((int)(endVersion - startVersion));
            await Task.Run(async () =>
            {
                var getTableListTask = tableInfo.TableRepository.GetTableListFromDb();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var tableList = getTableListTask.Result;
                if (latestTimestamp != 0)
                    tableList = tableList.Where(t => t.Version >= tableInfo.GetVersion(latestTimestamp)).ToList();
                using (var conn = tableInfo.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    foreach (var table in tableList)
                    {
                        var sql = $"COPY (SELECT stateid,typecode,data,version,timestamp from {table.Name} WHERE stateid='{stateId.ToString()}' and version>{startVersion} and version<={endVersion} order by version asc) TO STDOUT (FORMAT BINARY)";
                        using (var reader = conn.BeginBinaryExport(sql))
                        {
                            while (reader.StartRow() != -1)
                            {
                                originList.Add(new EventBytesWrapper
                                {
                                    StateId = reader.Read<string>(NpgsqlDbType.Varchar),
                                    TypeCode = reader.Read<string>(NpgsqlDbType.Varchar),
                                    Data = reader.Read<byte[]>(NpgsqlDbType.Bytea),
                                    Version = reader.Read<long>(NpgsqlDbType.Bigint),
                                    Timestamp = reader.Read<long>(NpgsqlDbType.Bigint)
                                });
                            }
                        }
                    }
                }
            });

            var list = new List<IFullyEvent<PrimaryKey>>(originList.Count);
            foreach (var origin in originList)
            {
                using (var ms = new MemoryStream(origin.Data))
                {
                    if (serializer.Deserialize(TypeContainer.GetType(origin.TypeCode), ms) is IEvent evt)
                    {
                        if (typeof(PrimaryKey) == typeof(long) && long.Parse(origin.StateId) is PrimaryKey actorIdWithLong)
                        {
                            list.Add(new FullyEvent<PrimaryKey>
                            {
                                StateId = actorIdWithLong,
                                Event = evt,
                                Base = new EventBase(origin.Version, origin.Timestamp)
                            });
                        }
                        else if(origin.StateId is PrimaryKey actorIdWithString)
                        {
                            list.Add(new FullyEvent<PrimaryKey>
                            {
                                StateId = actorIdWithString,
                                Event = evt,
                                Base = new EventBase(origin.Version, origin.Timestamp)
                            });
                        }
                    }
                }
            }
            return list.OrderBy(e => e.Base.Version).ToList();
        }
        public async Task<IList<IFullyEvent<PrimaryKey>>> GetListByType(PrimaryKey stateId, string typeCode, long startVersion, int limit)
        {
            var type = TypeContainer.GetType(typeCode);
            var originList = new List<EventBytesWrapper>(limit);
            await Task.Run(async () =>
            {
                var getTableListTask = tableInfo.TableRepository.GetTableListFromDb();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var tableList = getTableListTask.Result;
                using (var conn = tableInfo.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    foreach (var table in tableList)
                    {
                        var sql = $"COPY (SELECT stateid,typecode,data,version,timestamp from {table.Name} WHERE stateid='{stateId.ToString()}' and typecode='{typeCode}' and version>{startVersion} order by version asc limit {limit}) TO STDOUT (FORMAT BINARY)";
                        using (var reader = conn.BeginBinaryExport(sql))
                        {
                            while (reader.StartRow() != -1)
                            {
                                originList.Add(new EventBytesWrapper
                                {
                                    StateId = reader.Read<string>(NpgsqlDbType.Varchar),
                                    TypeCode = reader.Read<string>(NpgsqlDbType.Varchar),
                                    Data = reader.Read<byte[]>(NpgsqlDbType.Bytea),
                                    Version = reader.Read<long>(NpgsqlDbType.Bigint),
                                    Timestamp = reader.Read<long>(NpgsqlDbType.Bigint)
                                });
                            }
                        }
                        if (originList.Count >= limit)
                            break;
                    }
                }
            });
            var list = new List<IFullyEvent<PrimaryKey>>(originList.Count);
            foreach (var origin in originList)
            {
                using (var ms = new MemoryStream(origin.Data))
                {
                    if (serializer.Deserialize(type, ms) is IEvent evt)
                    {
                        if (typeof(PrimaryKey) == typeof(long) && long.Parse(origin.StateId) is PrimaryKey actorIdWithLong)
                        {
                            list.Add(new FullyEvent<PrimaryKey>
                            {
                                StateId = actorIdWithLong,
                                Event = evt,
                                Base = new EventBase(origin.Version, origin.Timestamp)
                            });
                        }
                        else if (origin.StateId is PrimaryKey actorIdWithString)
                        {
                            list.Add(new FullyEvent<PrimaryKey>
                            {
                                StateId = actorIdWithString,
                                Event = evt,
                                Base = new EventBase(origin.Version, origin.Timestamp)
                            });
                        }
                    }
                }
            }
            return list.OrderBy(e => e.Base.Version).ToList();
        }

        static readonly ConcurrentDictionary<string, string> saveSqlDict = new ConcurrentDictionary<string, string>();
        public Task<bool> Append(SaveTransport<PrimaryKey> transport)
        {
            return Task.Run(async () =>
            {
                var wrap = new DataAsyncWrapper<SaveTransport<PrimaryKey>, bool>(transport);
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                    await writeTask;
                return await wrap.TaskSource.Task;
            });
        }
        private async Task BatchProcessing(List<DataAsyncWrapper<SaveTransport<PrimaryKey>, bool>> wrapperList)
        {
            var copySql = copySaveSqlDict.GetOrAdd((await tableInfo.GetTable(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())).Name,
                key => $"copy {key}(stateid,uniqueId,typecode,data,version,timestamp) FROM STDIN (FORMAT BINARY)");
            try
            {
                using (var conn = tableInfo.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    using (var writer = conn.BeginBinaryImport(copySql))
                    {
                        foreach (var wrapper in wrapperList)
                        {
                            writer.StartRow();
                            writer.Write(wrapper.Value.Event.StateId.ToString(), NpgsqlDbType.Varchar);
                            writer.Write(wrapper.Value.UniqueId, NpgsqlDbType.Varchar);
                            writer.Write(wrapper.Value.Event.GetType().FullName, NpgsqlDbType.Varchar);
                            writer.Write(wrapper.Value.BytesTransport.EventBytes, NpgsqlDbType.Bytea);
                            writer.Write(wrapper.Value.Event.Base.Version, NpgsqlDbType.Bigint);
                            writer.Write(wrapper.Value.Event.Base.Timestamp, NpgsqlDbType.Bigint);
                        }
                        writer.Complete();
                    }
                }
                wrapperList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
            }
            catch
            {
                var saveSql = await GetInsertSql();
                using (var conn = tableInfo.CreateConnection())
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
                                    StateId = wrapper.Value.Event.StateId.ToString(),
                                    wrapper.Value.UniqueId,
                                    TypeCode = wrapper.Value.Event.Event.GetType().FullName,
                                    Data = wrapper.Value.BytesTransport.EventBytes,
                                    wrapper.Value.Event.Base.Version,
                                    wrapper.Value.Event.Base.Timestamp
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask<string> GetInsertSql()
        {
            return saveSqlDict.GetOrAdd((await tableInfo.GetTable(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())).Name,
                key => $"INSERT INTO {key}(stateid,uniqueId,typecode,data,version,timestamp) VALUES(@StateId,@UniqueId,@TypeCode,@Data,@Version,@Timestamp) ON CONFLICT ON CONSTRAINT {key}_id_unique DO NOTHING");
        }
        static readonly ConcurrentDictionary<string, string> copySaveSqlDict = new ConcurrentDictionary<string, string>();
        public async Task TransactionBatchAppend(List<TransactionTransport<PrimaryKey>> list)
        {
            var getTableTask = tableInfo.GetTable(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            if (!getTableTask.IsCompletedSuccessfully)
                await getTableTask;
            var saveSql = copySaveSqlDict.GetOrAdd(getTableTask.Result.Name,
                key => $"copy {key}(stateid,uniqueId,typecode,data,version,timestamp) FROM STDIN (FORMAT BINARY)");
            await Task.Run(async () =>
            {
                using (var conn = tableInfo.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    using (var writer = conn.BeginBinaryImport(saveSql))
                    {
                        foreach (var wrapper in list)
                        {
                            writer.StartRow();
                            writer.Write(wrapper.FullyEvent.StateId.ToString(), NpgsqlDbType.Varchar);
                            writer.Write(wrapper.UniqueId, NpgsqlDbType.Varchar);
                            writer.Write(wrapper.FullyEvent.Event.GetType().FullName, NpgsqlDbType.Varchar);
                            writer.Write(wrapper.BytesTransport.EventBytes, NpgsqlDbType.Bytea);
                            writer.Write(wrapper.FullyEvent.Base.Version, NpgsqlDbType.Bigint);
                            writer.Write(wrapper.FullyEvent.Base.Timestamp, NpgsqlDbType.Bigint);
                        }
                        writer.Complete();
                    }
                }
            });
        }

        public async Task Delete(PrimaryKey stateId, long endVersion)
        {
            await Task.Run(async () =>
            {
                var getTableListTask = tableInfo.TableRepository.GetTableListFromDb();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var tableList = getTableListTask.Result;
                using (var conn = tableInfo.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    foreach (var table in tableList)
                    {
                        var sql = $"delete from {table.Name} WHERE stateid=@StateId and version<=@EndVersion";
                        await conn.ExecuteAsync(sql, new { StateId = stateId.ToString(), EndVersion = endVersion });
                    }
                }
            });
        }
    }
}
