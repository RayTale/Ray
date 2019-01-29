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
    public class SqlEventStorage<K> : IEventStorage<K>
    {
        readonly StorageConfig tableInfo;
        readonly IMpscChannel<DataAsyncWrapper<EventSaveWrapper<K>, bool>> mpscChannel;
        readonly ILogger<SqlEventStorage<K>> logger;
        readonly ISerializer serializer;
        public SqlEventStorage(IServiceProvider serviceProvider, StorageConfig tableInfo)
        {
            logger = serviceProvider.GetService<ILogger<SqlEventStorage<K>>>();
            serializer = serviceProvider.GetService<ISerializer>();
            mpscChannel = serviceProvider.GetService<IMpscChannel<DataAsyncWrapper<EventSaveWrapper<K>, bool>>>().BindConsumer(BatchProcessing);
            mpscChannel.ActiveConsumer();
            this.tableInfo = tableInfo;
        }
        public async Task<IList<IEvent<K>>> GetList(K stateId, long latestTimestamp, long startVersion, long endVersion)
        {
            var originList = new List<EventBytesWrapper>((int)(endVersion - startVersion));
            await Task.Run(async () =>
            {
                var getTableListTask = tableInfo.TableRepository.GetTableListFromDb();
                if (!getTableListTask.IsCompleted)
                    await getTableListTask;
                var tableList = getTableListTask.Result;
                if (latestTimestamp != 0)
                    tableList = tableList.Where(t => t.Version >= tableInfo.GetVersion(latestTimestamp)).ToList();
                using (var conn = tableInfo.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    foreach (var table in tableList)
                    {
                        var sql = $"COPY (SELECT typecode,data from {table.Name} WHERE stateid='{stateId.ToString()}' and version>{startVersion} and version<={endVersion} order by version asc) TO STDOUT (FORMAT BINARY)";
                        using (var reader = conn.BeginBinaryExport(sql))
                        {
                            while (reader.StartRow() != -1)
                            {
                                originList.Add(new EventBytesWrapper { TypeCode = reader.Read<string>(NpgsqlDbType.Varchar), Data = reader.Read<byte[]>(NpgsqlDbType.Bytea) });
                            }
                        }
                    }
                }
            });

            var list = new List<IEvent<K>>(originList.Count);
            foreach (var origin in originList)
            {
                using (var ms = new MemoryStream(origin.Data))
                {
                    if (serializer.Deserialize(TypeContainer.GetType(origin.TypeCode), ms) is IEvent<K> evt)
                    {
                        list.Add(evt);
                    }
                }
            }
            return list.OrderBy(e => e.GetBase().Version).ToList();
        }
        public async Task<IList<IEvent<K>>> GetListByType(K stateId, string typeCode, long startVersion, int limit)
        {
            var type = TypeContainer.GetType(typeCode);
            var originList = new List<byte[]>(limit);
            await Task.Run(async () =>
            {
                var getTableListTask = tableInfo.TableRepository.GetTableListFromDb();
                if (!getTableListTask.IsCompleted)
                    await getTableListTask;
                var tableList = getTableListTask.Result;
                using (var conn = tableInfo.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    foreach (var table in tableList)
                    {
                        var sql = $"COPY (SELECT data from {table.Name} WHERE stateid='{stateId.ToString()}' and typecode='{typeCode}' and version>{startVersion} order by version asc limit {limit}) TO STDOUT (FORMAT BINARY)";
                        using (var reader = conn.BeginBinaryExport(sql))
                        {
                            while (reader.StartRow() != -1)
                            {
                                originList.Add(reader.Read<byte[]>(NpgsqlDbType.Bytea));
                            }
                        }
                        if (originList.Count >= limit)
                            break;
                    }
                }
            });
            var list = new List<IEvent<K>>(originList.Count);
            foreach (var origin in originList)
            {
                using (var ms = new MemoryStream(origin))
                {
                    if (serializer.Deserialize(type, ms) is IEvent<K> evt)
                    {
                        list.Add(evt);
                    }
                }
            }
            return list.OrderBy(e => e.GetBase().Version).ToList();
        }

        static readonly ConcurrentDictionary<string, string> saveSqlDict = new ConcurrentDictionary<string, string>();
        public Task<bool> Append(IEvent<K> evt, byte[] bytes, string uniqueId = null)
        {
            return Task.Run(async () =>
            {
                var wrap = new DataAsyncWrapper<EventSaveWrapper<K>, bool>(new EventSaveWrapper<K>(evt, bytes, uniqueId));
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompleted)
                    await writeTask;
                return await wrap.TaskSource.Task;
            });
        }
        private async Task BatchProcessing(List<DataAsyncWrapper<EventSaveWrapper<K>, bool>> wrapperList)
        {
            var copySql = copySaveSqlDict.GetOrAdd((await tableInfo.GetTable(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())).Name, key => $"copy {key}(stateid,uniqueId,typecode,data,version) FROM STDIN (FORMAT BINARY)");
            try
            {
                using (var conn = tableInfo.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    using (var writer = conn.BeginBinaryImport(copySql))
                    {
                        foreach (var wrapper in wrapperList)
                        {
                            var eventBase = wrapper.Value.Event.GetBase();
                            writer.StartRow();
                            writer.Write(eventBase.StateId.ToString(), NpgsqlDbType.Varchar);
                            writer.Write(wrapper.Value.UniqueId, NpgsqlDbType.Varchar);
                            writer.Write(wrapper.Value.Event.GetType().FullName, NpgsqlDbType.Varchar);
                            writer.Write(wrapper.Value.Bytes, NpgsqlDbType.Bytea);
                            writer.Write(eventBase.Version, NpgsqlDbType.Bigint);
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
                                var eventBase = wrapper.Value.Event.GetBase();
                                wrapper.Value.ReturnValue = await conn.ExecuteAsync(saveSql, new
                                {
                                    StateId = eventBase.StateId.ToString(),
                                    wrapper.Value.UniqueId,
                                    TypeCode = wrapper.Value.Event.GetType().FullName,
                                    Data = wrapper.Value.Bytes,
                                    eventBase.Version
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
                key => $"INSERT INTO {key}(stateid,uniqueId,typecode,data,version) VALUES(@StateId,@UniqueId,@TypeCode,@Data,@Version) ON CONFLICT ON CONSTRAINT {key}_id_unique DO NOTHING");
        }
        static readonly ConcurrentDictionary<string, string> copySaveSqlDict = new ConcurrentDictionary<string, string>();
        public async Task TransactionBatchAppend(List<EventTransmitWrapper<K>> list)
        {
            var getTableTask = tableInfo.GetTable(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            if (!getTableTask.IsCompleted)
                await getTableTask;
            var saveSql = copySaveSqlDict.GetOrAdd(getTableTask.Result.Name,
                key => $"copy {key}(stateid,uniqueId,typecode,data,version) FROM STDIN (FORMAT BINARY)");
            await Task.Run(async () =>
            {
                using (var conn = tableInfo.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    using (var writer = conn.BeginBinaryImport(saveSql))
                    {
                        foreach (var wrapper in list)
                        {
                            var eventBase = wrapper.Evt.GetBase();
                            writer.StartRow();
                            writer.Write(eventBase.StateId.ToString(), NpgsqlDbType.Varchar);
                            writer.Write(wrapper.UniqueId, NpgsqlDbType.Varchar);
                            writer.Write(wrapper.Evt.GetType().FullName, NpgsqlDbType.Varchar);
                            writer.Write(wrapper.Bytes, NpgsqlDbType.Bytea);
                            writer.Write(eventBase.Version, NpgsqlDbType.Bigint);
                        }
                        writer.Complete();
                    }
                }
            });
        }

        public async Task Delete(K stateId, long endVersion)
        {
            await Task.Run(async () =>
            {
                var getTableListTask = tableInfo.TableRepository.GetTableListFromDb();
                if (!getTableListTask.IsCompleted)
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
