using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Abstractions;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Serialization;
using Ray.Core.Storage;
using Ray.Storage.SQLCore;
using Ray.Storage.SQLCore.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqlBulkCopy = Microsoft.Data.SqlClient.SqlBulkCopy;
using SqlConnection = Microsoft.Data.SqlClient.SqlConnection;

namespace Ray.Storage.SQLServer
{
    public class EventStorage<PrimaryKey> : IEventStorage<PrimaryKey>
    {
        readonly StorageOptions config;
        readonly IMpscChannel<AskInputBox<EventTaskBox<PrimaryKey>, bool>> mpscChannel;
        readonly ILogger<EventStorage<PrimaryKey>> logger;
        readonly ISerializer serializer;
        readonly ITypeFinder typeFinder;
        public EventStorage(IServiceProvider serviceProvider, StorageOptions config)
        {
            logger = serviceProvider.GetService<ILogger<EventStorage<PrimaryKey>>>();
            serializer = serviceProvider.GetService<ISerializer>();
            typeFinder = serviceProvider.GetService<ITypeFinder>();
            mpscChannel = serviceProvider.GetService<IMpscChannel<AskInputBox<EventTaskBox<PrimaryKey>, bool>>>();
            mpscChannel.BindConsumer(BatchInsertExecuter);
            this.config = config;
        }
        public async Task<IList<FullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion)
        {
            var list = new List<FullyEvent<PrimaryKey>>((int)(endVersion - startVersion));
            await Task.Run(async () =>
            {
                var getTableListTask = config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var stateIdStr = typeof(PrimaryKey) == typeof(long) ? stateId.ToString() : $"'{stateId.ToString()}'";
                using var conn = config.CreateConnection();
                await conn.OpenAsync();
                foreach (var table in getTableListTask.Result.Where(t => t.EndTime >= latestTimestamp))
                {
                    var sql = $"SELECT typecode,data,version,timestamp from {table.SubTable} WHERE stateid=@StateId and version>=@StartVersion and version<=@EndVersion order by version asc";
                    var originList = await conn.QueryAsync<EventModel>(sql, new
                    {
                        StateId = stateId,
                        StartVersion = startVersion,
                        EndVersion = endVersion
                    });
                    foreach (var item in originList)
                    {
                        if (serializer.Deserialize(Encoding.UTF8.GetBytes(item.Data), typeFinder.FindType(item.TypeCode)) is IEvent evt)
                        {
                            list.Add(new FullyEvent<PrimaryKey>
                            {
                                StateId = stateId,
                                Event = evt,
                                BasicInfo = new EventBasicInfo(item.Version, item.Timestamp)
                            });
                        }
                    }
                }
            });
            return list.OrderBy(e => e.BasicInfo.Version).ToList();
        }
        public async Task<IList<FullyEvent<PrimaryKey>>> GetListByType(PrimaryKey stateId, string typeCode, long startVersion, int limit)
        {
            var type = typeFinder.FindType(typeCode);
            var list = new List<FullyEvent<PrimaryKey>>(limit);
            await Task.Run((Func<Task>)(async () =>
            {
                var getTableListTask = config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                using var conn = config.CreateConnection();
                await conn.OpenAsync();
                foreach (var table in getTableListTask.Result)
                {
                    var sql = $"SELECT top(@Limit) data,version,timestamp from {table.SubTable} WHERE stateid=@StateId and typecode=@TypeCode and version>=@StartVersion order by version asc";
                    var originList = await conn.QueryAsync<EventModel>(sql, new
                    {
                        StateId = stateId,
                        TypeCode = typeCode,
                        StartVersion = startVersion,
                        Limit = limit
                    });
                    foreach (var item in originList)
                    {
                        if (serializer.Deserialize(Encoding.UTF8.GetBytes(item.Data), type) is IEvent evt)
                        {
                            list.Add(new FullyEvent<PrimaryKey>
                            {
                                StateId = stateId,
                                Event = evt,
                                BasicInfo = new EventBasicInfo(item.Version, item.Timestamp)
                            });
                        }
                    }
                    if (list.Count >= limit)
                        break;
                }
            }));
            return list.OrderBy(e => e.BasicInfo.Version).ToList();
        }

        static readonly ConcurrentDictionary<string, string> saveSqlDict = new ConcurrentDictionary<string, string>();
        public Task<bool> Append(FullyEvent<PrimaryKey> fullyEvent, string eventJson, string unique)
        {
            var input = new EventTaskBox<PrimaryKey>(fullyEvent, eventJson, unique);
            return Task.Run(async () =>
            {
                var wrap = new AskInputBox<EventTaskBox<PrimaryKey>, bool>(input);
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                    await writeTask;
                return await wrap.TaskSource.Task;
            });
        }
        private async Task BatchInsertExecuter(List<AskInputBox<EventTaskBox<PrimaryKey>, bool>> wrapperList)
        {
            var minTimestamp = wrapperList.Min(t => t.Value.Event.BasicInfo.Timestamp);
            var maxTimestamp = wrapperList.Max(t => t.Value.Event.BasicInfo.Timestamp);
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
                    var task = config.GetTable(t.Value.Event.BasicInfo.Timestamp);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    return (task.Result.SubTable, t);
                }))).GroupBy(t => t.SubTable);
                foreach (var group in groups)
                {
                    await BatchCopy(group.Key, group.Select(t => t.t).ToList());
                }
            }
            async Task BatchCopy(string tableName, List<AskInputBox<EventTaskBox<PrimaryKey>, bool>> list)
            {
                try
                {
                    using var conn = config.CreateConnection() as SqlConnection;
                    using var bulkCopy = new SqlBulkCopy(conn)
                    {
                        DestinationTableName = tableName,
                        BatchSize = wrapperList.Count
                    };
                    using var dt = new DataTable();
                    dt.Columns.Add("stateid", typeof(PrimaryKey));
                    dt.Columns.Add("uniqueId", typeof(string));
                    dt.Columns.Add("typecode", typeof(string));
                    dt.Columns.Add("data", typeof(string));
                    dt.Columns.Add("version", typeof(long));
                    dt.Columns.Add("timestamp", typeof(long));
                    foreach (var item in wrapperList)
                    {
                        var row = dt.NewRow();
                        row["stateid"] = item.Value.Event.StateId;
                        row["uniqueId"] = item.Value.UniqueId;
                        row["typecode"] = typeFinder.GetCode(item.Value.Event.Event.GetType());
                        row["data"] = item.Value.EventUtf8String;
                        row["version"] = item.Value.Event.BasicInfo.Version;
                        row["timestamp"] = item.Value.Event.BasicInfo.Timestamp;
                        dt.Rows.Add(row);
                    }
                    await conn.OpenAsync();
                    await bulkCopy.WriteToServerAsync(dt);
                    list.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, ex.Message);
                    var saveSql = saveSqlDict.GetOrAdd(tableName,
                        key => $"if NOT EXISTS(select * from {key} where StateId=@StateId and TypeCode=@TypeCode and UniqueId=@UniqueId)INSERT INTO {key}(stateid,uniqueId,typecode,data,version,timestamp) VALUES(@StateId,@UniqueId,@TypeCode,@Data,@Version,@Timestamp)");
                    await BatchInsert(saveSql, wrapperList);
                }
            }
            async Task BatchInsert(string saveSql, List<AskInputBox<EventTaskBox<PrimaryKey>, bool>> list)
            {
                bool isSuccess = false;
                using var conn = config.CreateConnection();
                await conn.OpenAsync();
                using var trans = conn.BeginTransaction();
                try
                {
                    foreach (var wrapper in list)
                    {
                        wrapper.Value.ReturnValue = await conn.ExecuteAsync(saveSql, new
                        {
                            StateId = wrapper.Value.Event.StateId.ToString(),
                            wrapper.Value.UniqueId,
                            TypeCode = typeFinder.GetCode(wrapper.Value.Event.Event.GetType()),
                            Data = wrapper.Value.EventUtf8String,
                            wrapper.Value.Event.BasicInfo.Version,
                            wrapper.Value.Event.BasicInfo.Timestamp
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
                if (!isSuccess)
                {
                    foreach (var wrapper in list)
                    {
                        try
                        {
                            wrapper.TaskSource.TrySetResult(await conn.ExecuteAsync(saveSql, new
                            {
                                wrapper.Value.Event.StateId,
                                wrapper.Value.UniqueId,
                                TypeCode = typeFinder.GetCode(wrapper.Value.Event.Event.GetType()),
                                Data = wrapper.Value.EventUtf8String,
                                wrapper.Value.Event.BasicInfo.Version,
                                wrapper.Value.Event.BasicInfo.Timestamp
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
        static readonly ConcurrentDictionary<string, string> copySaveSqlDict = new ConcurrentDictionary<string, string>();
        public async Task TransactionBatchAppend(List<EventBox<PrimaryKey>> list)
        {
            var minTimestamp = list.Min(t => t.FullyEvent.BasicInfo.Timestamp);
            var maxTimestamp = list.Max(t => t.FullyEvent.BasicInfo.Timestamp);
            var minTask = config.GetTable(minTimestamp);
            if (!minTask.IsCompletedSuccessfully)
                await minTask;
            if (minTask.Result.EndTime > maxTimestamp)
            {
                await Task.Run(async () =>
                {
                    using var conn = config.CreateConnection() as SqlConnection;
                    using var bulkCopy = new SqlBulkCopy(conn)
                    {
                        DestinationTableName = minTask.Result.SubTable,
                        BatchSize = list.Count
                    };
                    using var dt = new DataTable();
                    dt.Columns.Add("stateid", typeof(PrimaryKey));
                    dt.Columns.Add("uniqueId", typeof(string));
                    dt.Columns.Add("typecode", typeof(string));
                    dt.Columns.Add("data", typeof(string));
                    dt.Columns.Add("version", typeof(long));
                    dt.Columns.Add("timestamp", typeof(long));
                    foreach (var item in list)
                    {
                        var row = dt.NewRow();
                        row["stateid"] = item.FullyEvent.StateId;
                        row["uniqueId"] = item.UniqueId;
                        row["typecode"] = typeFinder.GetCode(item.FullyEvent.Event.GetType());
                        row["data"] = item.EventUtf8String;
                        row["version"] = item.FullyEvent.BasicInfo.Version;
                        row["timestamp"] = item.FullyEvent.BasicInfo.Timestamp;
                        dt.Rows.Add(row);
                    }
                    await conn.OpenAsync();
                    await bulkCopy.WriteToServerAsync(dt);
                });
            }
            else
            {
                var groups = (await Task.WhenAll(list.Select(async t =>
                {
                    var task = config.GetTable(t.FullyEvent.BasicInfo.Timestamp);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    return (task.Result.SubTable, t);
                }))).GroupBy(t => t.SubTable);
                using var conn = config.CreateConnection();
                await conn.OpenAsync();
                using var trans = conn.BeginTransaction();
                try
                {
                    foreach (var group in groups)
                    {
                        var saveSql = saveSqlDict.GetOrAdd(group.Key,
                            key => $"INSERT INTO {key}(stateid,uniqueId,typecode,data,version,timestamp) VALUES(@StateId,@UniqueId,@TypeCode,@Data,@Version,@Timestamp)");
                        await conn.ExecuteAsync(saveSql, group.Select(g => new
                        {
                            g.t.FullyEvent.StateId,
                            g.t.UniqueId,
                            TypeCode = typeFinder.GetCode(g.t.FullyEvent.Event.GetType()),
                            Data = g.t.EventUtf8String,
                            g.t.FullyEvent.BasicInfo.Version,
                            g.t.FullyEvent.BasicInfo.Timestamp
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

        public Task DeletePrevious(PrimaryKey stateId, long toVersion, long startTimestamp)
        {
            return Task.Run(async () =>
            {
                var getTableListTask = config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var tableList = getTableListTask.Result.Where(t => t.EndTime >= startTimestamp);
                using var conn = config.CreateConnection();
                await conn.OpenAsync();
                using var trans = conn.BeginTransaction();
                try
                {
                    foreach (var table in tableList)
                    {
                        var sql = $"delete from {table.SubTable} WHERE stateid=@StateId and version<=@EndVersion";
                        await conn.ExecuteAsync(sql, new { StateId = stateId, EndVersion = toVersion }, transaction: trans);
                    }
                    trans.Commit();
                }
                catch
                {
                    trans.Rollback();
                    throw;
                }
            });
        }

        public Task DeleteAfter(PrimaryKey stateId, long fromVersion, long startTimestamp)
        {
            return Task.Run(async () =>
            {
                var getTableListTask = config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var tableList = getTableListTask.Result.Where(t => t.EndTime >= startTimestamp);
                using var conn = config.CreateConnection();
                await conn.OpenAsync();
                using var trans = conn.BeginTransaction();
                try
                {
                    foreach (var table in tableList)
                    {
                        var sql = $"delete from {table.SubTable} WHERE stateid=@StateId and version>=@StartVersion";
                        await conn.ExecuteAsync(sql, new { StateId = stateId, StartVersion = fromVersion });
                    }
                    trans.Commit();
                }
                catch
                {
                    trans.Rollback();
                    throw;
                }
            });
        }

        public Task DeleteByVersion(PrimaryKey stateId, long version, long timestamp)
        {
            return Task.Run(async () =>
            {
                var getTableListTask = config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var table = getTableListTask.Result.SingleOrDefault(t => t.StartTime <= timestamp && t.EndTime >= timestamp);
                using var conn = config.CreateConnection();
                var sql = $"delete from {table.SubTable} WHERE stateid=@StateId and version=@Version";
                await conn.ExecuteAsync(sql, new { StateId = stateId, Version = version });
            });
        }
    }
}
