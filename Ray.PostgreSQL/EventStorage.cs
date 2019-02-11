using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
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
    public class EventStorage<PrimaryKey> : IEventStorage<PrimaryKey>
    {
        readonly StorageConfig tableInfo;
        readonly IMpscChannel<DataAsyncWrapper<SaveTransport<PrimaryKey>, bool>> mpscChannel;
        readonly ILogger<EventStorage<PrimaryKey>> logger;
        readonly ISerializer serializer;
        public EventStorage(IServiceProvider serviceProvider, StorageConfig tableInfo)
        {
            logger = serviceProvider.GetService<ILogger<EventStorage<PrimaryKey>>>();
            serializer = serviceProvider.GetService<ISerializer>();
            mpscChannel = serviceProvider.GetService<IMpscChannel<DataAsyncWrapper<SaveTransport<PrimaryKey>, bool>>>().BindConsumer(BatchProcessing);
            mpscChannel.ActiveConsumer();
            this.tableInfo = tableInfo;
        }
        public async Task<IList<IFullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion)
        {
            var list = new List<IFullyEvent<PrimaryKey>>((int)(endVersion - startVersion));
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
                        var sql = $"COPY (SELECT typecode,data,version,timestamp from {table.Name} WHERE stateid='{stateId.ToString()}' and version>={startVersion} and version<={endVersion} order by version asc) TO STDOUT (FORMAT BINARY)";
                        using (var reader = conn.BeginBinaryExport(sql))
                        {
                            while (reader.StartRow() != -1)
                            {
                                var typeCode = reader.Read<string>(NpgsqlDbType.Varchar);
                                var data = reader.Read<string>(NpgsqlDbType.Jsonb);
                                var version = reader.Read<long>(NpgsqlDbType.Bigint);
                                var timestamp = reader.Read<long>(NpgsqlDbType.Bigint);
                                if (version <= endVersion && version >= startVersion)
                                {
                                    if (serializer.Deserialize(TypeContainer.GetType(typeCode), Encoding.Default.GetBytes(data)) is IEvent evt)
                                    {
                                        list.Add(new FullyEvent<PrimaryKey>
                                        {
                                            StateId = stateId,
                                            Event = evt,
                                            Base = new EventBase(version, timestamp)
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            });
            return list.OrderBy(e => e.Base.Version).ToList();
        }
        public async Task<IList<IFullyEvent<PrimaryKey>>> GetListByType(PrimaryKey stateId, string typeCode, long startVersion, int limit)
        {
            var type = TypeContainer.GetType(typeCode);
            var list = new List<IFullyEvent<PrimaryKey>>(limit);
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
                        var sql = $"COPY (SELECT data,version,timestamp from {table.Name} WHERE stateid='{stateId.ToString()}' and typecode='{typeCode}' and version>={startVersion} order by version asc limit {limit}) TO STDOUT (FORMAT BINARY)";
                        using (var reader = conn.BeginBinaryExport(sql))
                        {
                            while (reader.StartRow() != -1)
                            {
                                var data = reader.Read<string>(NpgsqlDbType.Jsonb);
                                var version = reader.Read<long>(NpgsqlDbType.Bigint);
                                var timestamp = reader.Read<long>(NpgsqlDbType.Bigint);
                                if (version >= startVersion && serializer.Deserialize(type, Encoding.Default.GetBytes(data)) is IEvent evt)
                                {
                                    list.Add(new FullyEvent<PrimaryKey>
                                    {
                                        StateId = stateId,
                                        Event = evt,
                                        Base = new EventBase(version, timestamp)
                                    });
                                }
                            }
                        }
                        if (list.Count >= limit)
                            break;
                    }
                }
            });
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
                            writer.Write(Encoding.Default.GetString(wrapper.Value.BytesTransport.EventBytes), NpgsqlDbType.Jsonb);
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
                                    Data = Encoding.Default.GetString(wrapper.Value.BytesTransport.EventBytes),
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
                key => $"INSERT INTO {key}(stateid,uniqueId,typecode,data,version,timestamp) VALUES(@StateId,@UniqueId,@TypeCode,(@Data)::jsonb,@Version,@Timestamp) ON CONFLICT ON CONSTRAINT {key}_id_unique DO NOTHING");
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
                            writer.Write(Encoding.Default.GetString(wrapper.BytesTransport.EventBytes), NpgsqlDbType.Jsonb);
                            writer.Write(wrapper.FullyEvent.Base.Version, NpgsqlDbType.Bigint);
                            writer.Write(wrapper.FullyEvent.Base.Timestamp, NpgsqlDbType.Bigint);
                        }
                        writer.Complete();
                    }
                }
            });
        }

        public async Task Delete(PrimaryKey stateId, long endVersion, long startTimestamp)
        {
            await Task.Run(async () =>
            {
                var getTableListTask = tableInfo.TableRepository.GetTableListFromDb();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var tableList = getTableListTask.Result.Where(t => t.CreateTime >= startTimestamp);
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
