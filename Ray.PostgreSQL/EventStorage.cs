using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
        readonly StorageConfig config;
        readonly IMpscChannel<DataAsyncWrapper<BatchAppendTransport<PrimaryKey>, bool>> mpscChannel;
        readonly ILogger<EventStorage<PrimaryKey>> logger;
        readonly ISerializer serializer;
        public EventStorage(IServiceProvider serviceProvider, StorageConfig config)
        {
            logger = serviceProvider.GetService<ILogger<EventStorage<PrimaryKey>>>();
            serializer = serviceProvider.GetService<ISerializer>();
            mpscChannel = serviceProvider.GetService<IMpscChannel<DataAsyncWrapper<BatchAppendTransport<PrimaryKey>, bool>>>().BindConsumer(BatchProcessing);
            mpscChannel.ActiveConsumer();
            this.config = config;
        }
        public async Task<IList<IFullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion)
        {
            var list = new List<IFullyEvent<PrimaryKey>>((int)(endVersion - startVersion));
            await Task.Run(async () =>
            {
                var getTableListTask = config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                using (var conn = config.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    foreach (var table in getTableListTask.Result.Where(t => t.EndTime >= latestTimestamp))
                    {
                        var sql = $"COPY (SELECT typecode,data,version,timestamp from {table.SubTable} WHERE stateid='{stateId.ToString()}' and version>={startVersion} and version<={endVersion} order by version asc) TO STDOUT (FORMAT BINARY)";
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
                var getTableListTask = config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                using (var conn = config.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    foreach (var table in getTableListTask.Result)
                    {
                        var sql = $"COPY (SELECT data,version,timestamp from {table.SubTable} WHERE stateid='{stateId.ToString()}' and typecode='{typeCode}' and version>={startVersion} order by version asc limit {limit}) TO STDOUT (FORMAT BINARY)";
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
        public Task<bool> Append(IFullyEvent<PrimaryKey> fullyEvent, EventBytesTransport bytesTransport, string unique)
        {
            return Task.Run(async () =>
            {
                var wrap = new DataAsyncWrapper<BatchAppendTransport<PrimaryKey>, bool>(new BatchAppendTransport<PrimaryKey>(fullyEvent, bytesTransport, unique));
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                    await writeTask;
                return await wrap.TaskSource.Task;
            });
        }
        private async Task BatchProcessing(List<DataAsyncWrapper<BatchAppendTransport<PrimaryKey>, bool>> wrapperList)
        {
            var minTimestamp = wrapperList.Min(t => t.Value.Event.Base.Timestamp);
            var maxTimestamp = wrapperList.Max(t => t.Value.Event.Base.Timestamp);
            var minTask = config.GetTable(minTimestamp);
            if (!minTask.IsCompletedSuccessfully)
                await minTask;
            if (minTask.Result.EndTime > maxTimestamp)
            {
                await BatchCopy(minTask.Result.SubTable, wrapperList);
            }
            else
            {
                var groups = (await Task.WhenAll(wrapperList.Select(async t =>
                {
                    var task = config.GetTable(t.Value.Event.Base.Timestamp);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    return (task.Result.SubTable, t);
                }))).GroupBy(t => t.SubTable);
                foreach (var group in groups)
                {
                    await BatchCopy(group.Key, group.Select(t => t.t).ToList());
                }
            }
            async Task BatchCopy(string tableName, List<DataAsyncWrapper<BatchAppendTransport<PrimaryKey>, bool>> list)
            {
                try
                {
                    var copySql = copySaveSqlDict.GetOrAdd(tableName,
                         key => $"copy {key}(stateid,uniqueId,typecode,data,version,timestamp) FROM STDIN (FORMAT BINARY)");
                    using (var conn = config.CreateConnection() as NpgsqlConnection)
                    {
                        await conn.OpenAsync();
                        using (var writer = conn.BeginBinaryImport(copySql))
                        {
                            foreach (var wrapper in list)
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
                    list.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, ex.Message);
                    var saveSql = saveSqlDict.GetOrAdd(tableName,
                        key => $"INSERT INTO {key}(stateid,uniqueId,typecode,data,version,timestamp) VALUES(@StateId,@UniqueId,@TypeCode,(@Data)::jsonb,@Version,@Timestamp) ON CONFLICT ON CONSTRAINT {key}_id_unique DO NOTHING");
                    await BatchInsert(saveSql, wrapperList);
                }
            }
            async Task BatchInsert(string saveSql, List<DataAsyncWrapper<BatchAppendTransport<PrimaryKey>, bool>> list)
            {
                bool isSuccess = false;
                using (var conn = config.CreateConnection())
                {
                    await conn.OpenAsync();
                    using (var trans = conn.BeginTransaction())
                    {
                        try
                        {
                            foreach (var wrapper in list)
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
                            isSuccess = true;
                            list.ForEach(wrap => wrap.TaskSource.TrySetResult(wrap.Value.ReturnValue));
                        }
                        catch
                        {
                            trans.Rollback();
                        }
                    }
                    if (!isSuccess)
                    {
                        foreach (var wrapper in list)
                        {
                            try
                            {
                                wrapper.TaskSource.TrySetResult(await conn.ExecuteAsync(saveSql, new
                                {
                                    StateId = wrapper.Value.Event.StateId.ToString(),
                                    wrapper.Value.UniqueId,
                                    TypeCode = wrapper.Value.Event.Event.GetType().FullName,
                                    Data = Encoding.Default.GetString(wrapper.Value.BytesTransport.EventBytes),
                                    wrapper.Value.Event.Base.Version,
                                    wrapper.Value.Event.Base.Timestamp
                                }) > 0);
                            }
                            catch (Exception ex)
                            {
                                wrapper.TaskSource.TrySetException(ex);
                            }
                        }
                    }
                }
            }
        }
        static readonly ConcurrentDictionary<string, string> copySaveSqlDict = new ConcurrentDictionary<string, string>();
        public async Task TransactionBatchAppend(List<EventTransport<PrimaryKey>> list)
        {
            var minTimestamp = list.Min(t => t.FullyEvent.Base.Timestamp);
            var maxTimestamp = list.Max(t => t.FullyEvent.Base.Timestamp);
            var minTask = config.GetTable(minTimestamp);
            if (!minTask.IsCompletedSuccessfully)
                await minTask;
            if (minTask.Result.EndTime > maxTimestamp)
            {
                var saveSql = copySaveSqlDict.GetOrAdd(minTask.Result.SubTable,
                    key => $"copy {key}(stateid,uniqueId,typecode,data,version,timestamp) FROM STDIN (FORMAT BINARY)");
                await Task.Run(async () =>
                {
                    using (var conn = config.CreateConnection() as NpgsqlConnection)
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
            else
            {
                var groups = (await Task.WhenAll(list.Select(async t =>
                {
                    var task = config.GetTable(t.FullyEvent.Base.Timestamp);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    return (task.Result.SubTable, t);
                }))).GroupBy(t => t.SubTable);
                using (var conn = config.CreateConnection())
                {
                    await conn.OpenAsync();
                    using (var trans = conn.BeginTransaction())
                    {
                        try
                        {
                            foreach (var group in groups)
                            {
                                var saveSql = saveSqlDict.GetOrAdd(group.Key,
                                    key => $"INSERT INTO {key}(stateid,uniqueId,typecode,data,version,timestamp) VALUES(@StateId,@UniqueId,@TypeCode,(@Data)::jsonb,@Version,@Timestamp)");
                                await conn.ExecuteAsync(saveSql, group.Select(g => new
                                {
                                    StateId = g.t.FullyEvent.StateId.ToString(),
                                    g.t.UniqueId,
                                    TypeCode = g.t.FullyEvent.GetType().FullName,
                                    Data = Encoding.Default.GetString(g.t.BytesTransport.EventBytes),
                                    g.t.FullyEvent.Base.Version,
                                    g.t.FullyEvent.Base.Timestamp
                                }), trans);
                            }
                            trans.Commit();
                        }
                        catch (Exception ex)
                        {
                            trans.Rollback();
                            logger.LogError(ex, nameof(TransactionBatchAppend));
                            throw;
                        }
                    }
                }
            }
        }

        public Task DeleteStart(PrimaryKey stateId, long endVersion, long startTimestamp)
        {
            return Task.Run(async () =>
            {
                var getTableListTask = config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var tableList = getTableListTask.Result.Where(t => t.EndTime >= startTimestamp);
                using (var conn = config.CreateConnection())
                {
                    await conn.OpenAsync();
                    using (var trans = conn.BeginTransaction())
                    {
                        try
                        {
                            foreach (var table in tableList)
                            {
                                var sql = $"delete from {table.SubTable} WHERE stateid=@StateId and version<=@EndVersion";
                                await conn.ExecuteAsync(sql, new { StateId = stateId.ToString(), EndVersion = endVersion }, transaction: trans);
                            }
                            trans.Commit();
                        }
                        catch
                        {
                            trans.Rollback();
                            throw;
                        }
                    }
                }
            });
        }

        public Task DeleteEnd(PrimaryKey stateId, long startVersion, long startTimestamp)
        {
            return Task.Run(async () =>
            {
                var getTableListTask = config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var tableList = getTableListTask.Result.Where(t => t.EndTime >= startTimestamp);
                using (var conn = config.CreateConnection())
                {
                    await conn.OpenAsync();
                    using (var trans = conn.BeginTransaction())
                    {
                        try
                        {
                            foreach (var table in tableList)
                            {
                                var sql = $"delete from {table.SubTable} WHERE stateid=@StateId and version>=@StartVersion";
                                await conn.ExecuteAsync(sql, new { StateId = stateId.ToString(), StartVersion = startVersion });
                            }
                            trans.Commit();
                        }
                        catch
                        {
                            trans.Rollback();
                            throw;
                        }
                    }
                }
            });
        }
    }
}
