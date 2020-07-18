using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

namespace Ray.Storage.MySQL
{
    public class EventStorage<PrimaryKey> : IEventStorage<PrimaryKey>
    {
        private readonly StorageOptions config;
        private readonly IMpscChannel<AskInputBox<EventTaskBox<PrimaryKey>, bool>> mpscChannel;
        private readonly ILogger<EventStorage<PrimaryKey>> logger;
        private readonly ISerializer serializer;
        private readonly ITypeFinder typeFinder;

        public EventStorage(IServiceProvider serviceProvider, StorageOptions config)
        {
            this.logger = serviceProvider.GetService<ILogger<EventStorage<PrimaryKey>>>();
            this.serializer = serviceProvider.GetService<ISerializer>();
            this.typeFinder = serviceProvider.GetService<ITypeFinder>();
            this.mpscChannel = serviceProvider.GetService<IMpscChannel<AskInputBox<EventTaskBox<PrimaryKey>, bool>>>();
            this.mpscChannel.BindConsumer(this.BatchInsertExecuter);
            this.config = config;
        }

        private static readonly ConcurrentDictionary<string, string> getListSqlDict = new ConcurrentDictionary<string, string>();

        public async Task<IList<FullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion)
        {
            var list = new List<FullyEvent<PrimaryKey>>((int)(endVersion - startVersion));
            await Task.Run(async () =>
            {
                var getTableListTask = this.config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                {
                    await getTableListTask;
                }

                var stateIdStr = typeof(PrimaryKey) == typeof(long) ? stateId.ToString() : $"'{stateId.ToString()}'";
                using var conn = this.config.CreateConnection();
                await conn.OpenAsync();
                foreach (var table in getTableListTask.Result.Where(t => t.EndTime >= latestTimestamp))
                {
                    var sql = getListSqlDict.GetOrAdd(table.SubTable, key => $"SELECT TypeCode,Data,Version,Timestamp from {key} WHERE StateId=@StateId and Version>=@StartVersion and Version<=@EndVersion order by Version asc");
                    var originList = await conn.QueryAsync<EventModel>(sql, new
                    {
                        StateId = stateId,
                        StartVersion = startVersion,
                        EndVersion = endVersion
                    });
                    foreach (var item in originList)
                    {
                        if (this.serializer.Deserialize(Encoding.UTF8.GetBytes(item.Data), this.typeFinder.FindType(item.TypeCode)) is IEvent evt)
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

        private static readonly ConcurrentDictionary<string, string> getListByTypeSqlDict = new ConcurrentDictionary<string, string>();

        public async Task<IList<FullyEvent<PrimaryKey>>> GetListByType(PrimaryKey stateId, string typeCode, long startVersion, int limit)
        {
            var type = this.typeFinder.FindType(typeCode);
            var list = new List<FullyEvent<PrimaryKey>>(limit);
            await Task.Run(async () =>
            {
                var getTableListTask = this.config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                {
                    await getTableListTask;
                }

                using var conn = this.config.CreateConnection();
                await conn.OpenAsync();
                foreach (var table in getTableListTask.Result)
                {
                    var sql = getListByTypeSqlDict.GetOrAdd(table.SubTable, key => $"SELECT Data,Version,Timestamp from {key} WHERE StateId=@StateId and TypeCode=@TypeCode and Version>=@StartVersion order by Version asc limit @Limit");
                    var originList = await conn.QueryAsync<EventModel>(sql, new
                    {
                        StateId = stateId,
                        TypeCode = typeCode,
                        StartVersion = startVersion,
                        Limit = limit
                    });
                    foreach (var item in originList)
                    {
                        if (this.serializer.Deserialize(Encoding.UTF8.GetBytes(item.Data), type) is IEvent evt)
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
                    {
                        break;
                    }
                }
            });
            return list.OrderBy(e => e.BasicInfo.Version).ToList();
        }

        private static readonly ConcurrentDictionary<string, string> saveSqlDict = new ConcurrentDictionary<string, string>();
        private static readonly ConcurrentDictionary<string, string> copySaveSqlDict = new ConcurrentDictionary<string, string>();

        public Task<bool> Append(FullyEvent<PrimaryKey> fullyEvent, string eventJson, string unique)
        {
            var input = new EventTaskBox<PrimaryKey>(fullyEvent, eventJson, unique);
            return Task.Run(async () =>
            {
                var wrap = new AskInputBox<EventTaskBox<PrimaryKey>, bool>(input);
                var writeTask = this.mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                {
                    await writeTask;
                }

                return await wrap.TaskSource.Task;
            });
        }

        private string GetSaveSql(string tableName)
        {
            return saveSqlDict.GetOrAdd(
                tableName,
                             key => $"INSERT ignore INTO {key}(StateId,UniqueId,TypeCode,Data,Version,Timestamp) VALUES(@StateId,@UniqueId,@TypeCode,@Data,@Version,@Timestamp)");
        }

        private string GetCopySaveSql(string tableName)
        {
            return copySaveSqlDict.GetOrAdd(
                tableName,
                             key => $"INSERT INTO {key}(StateId,UniqueId,TypeCode,Data,Version,Timestamp) VALUES(@StateId,@UniqueId,@TypeCode,@Data,@Version,@Timestamp)");
        }

        private async Task BatchInsertExecuter(List<AskInputBox<EventTaskBox<PrimaryKey>, bool>> wrapperList)
        {
            var minTimestamp = wrapperList.Min(t => t.Value.Event.BasicInfo.Timestamp);
            var maxTimestamp = wrapperList.Max(t => t.Value.Event.BasicInfo.Timestamp);
            var minTask = this.config.GetTable(minTimestamp);
            if (!minTask.IsCompletedSuccessfully)
            {
                await minTask;
            }

            if (minTask.Result.EndTime > maxTimestamp)
            {
                await BatchCopy(minTask.Result.SubTable, wrapperList);
            }
            else
            {
                var groups = (await Task.WhenAll(wrapperList.Select(async t =>
                {
                    var task = this.config.GetTable(t.Value.Event.BasicInfo.Timestamp);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }

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
                    using var conn = this.config.CreateConnection();
                    await conn.ExecuteAsync(this.GetCopySaveSql(tableName), list.Select(wrapper => new
                    {
                        StateId = wrapper.Value.Event.StateId.ToString(),
                        wrapper.Value.UniqueId,
                        TypeCode = this.typeFinder.GetCode(wrapper.Value.Event.Event.GetType()),
                        Data = wrapper.Value.EventUtf8String,
                        wrapper.Value.Event.BasicInfo.Version,
                        wrapper.Value.Event.BasicInfo.Timestamp
                    }));
                    list.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
                }
                catch (Exception ex)
                {
                    this.logger.LogError(ex, ex.Message);
                    await BatchInsert(tableName, wrapperList);
                }
            }

            async Task BatchInsert(string tableName, List<AskInputBox<EventTaskBox<PrimaryKey>, bool>> list)
            {
                bool isSuccess = false;
                var saveSql = this.GetSaveSql(tableName);
                using var conn = this.config.CreateConnection();
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
                            TypeCode = this.typeFinder.GetCode(wrapper.Value.Event.Event.GetType()),
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
                                TypeCode = this.typeFinder.GetCode(wrapper.Value.Event.Event.GetType()),
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

        public async Task TransactionBatchAppend(List<EventBox<PrimaryKey>> list)
        {
            var minTimestamp = list.Min(t => t.FullyEvent.BasicInfo.Timestamp);
            var maxTimestamp = list.Max(t => t.FullyEvent.BasicInfo.Timestamp);
            var minTask = this.config.GetTable(minTimestamp);
            if (!minTask.IsCompletedSuccessfully)
            {
                await minTask;
            }

            var groups = (await Task.WhenAll(list.Select(async t =>
            {
                var task = this.config.GetTable(t.FullyEvent.BasicInfo.Timestamp);
                if (!task.IsCompletedSuccessfully)
                {
                    await task;
                }

                return (task.Result.SubTable, t);
            }))).GroupBy(t => t.SubTable);
            using var conn = this.config.CreateConnection();
            await conn.OpenAsync();
            using var trans = conn.BeginTransaction();
            try
            {
                foreach (var group in groups)
                {
                    await conn.ExecuteAsync(this.GetSaveSql(group.Key), group.Select(g => new
                    {
                        g.t.FullyEvent.StateId,
                        g.t.UniqueId,
                        TypeCode = this.typeFinder.GetCode(g.t.FullyEvent.Event.GetType()),
                        Data = g.t.EventUtf8String,
                        g.t.FullyEvent.BasicInfo.Version,
                        g.t.FullyEvent.BasicInfo.Timestamp
                    }).ToArray(), trans);
                }

                trans.Commit();
            }
            catch (Exception ex)
            {
                trans.Rollback();
                this.logger.LogError(ex, nameof(this.TransactionBatchAppend));
                throw;
            }
        }

        public Task DeletePrevious(PrimaryKey stateId, long toVersion, long startTimestamp)
        {
            return Task.Run(async () =>
            {
                var getTableListTask = this.config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                {
                    await getTableListTask;
                }

                var tableList = getTableListTask.Result.Where(t => t.EndTime >= startTimestamp);
                using var conn = this.config.CreateConnection();
                await conn.OpenAsync();
                using var trans = conn.BeginTransaction();
                try
                {
                    foreach (var table in tableList)
                    {
                        var sql = $"delete from {table.SubTable} WHERE StateId=@StateId and Version<=@EndVersion";
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
                var getTableListTask = this.config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                {
                    await getTableListTask;
                }

                var tableList = getTableListTask.Result.Where(t => t.EndTime >= startTimestamp);
                using var conn = this.config.CreateConnection();
                await conn.OpenAsync();
                using var trans = conn.BeginTransaction();
                try
                {
                    foreach (var table in tableList)
                    {
                        var sql = $"delete from {table.SubTable} WHERE StateId=@StateId and Version>=@StartVersion";
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
                var getTableListTask = this.config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                {
                    await getTableListTask;
                }

                var table = getTableListTask.Result.SingleOrDefault(t => t.StartTime <= timestamp && t.EndTime >= timestamp);
                using var conn = this.config.CreateConnection();
                var sql = $"delete from {table.SubTable} WHERE StateId=@StateId and Version=@Version";
                await conn.ExecuteAsync(sql, new { StateId = stateId, Version = version });
            });
        }
    }
}
