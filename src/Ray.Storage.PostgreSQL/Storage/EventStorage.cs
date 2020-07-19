﻿using System;
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
using Ray.Core.Abstractions;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Serialization;
using Ray.Core.Storage;
using Ray.Storage.SQLCore.Configuration;

namespace Ray.Storage.PostgreSQL
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

                var stateIdStr = typeof(PrimaryKey) == typeof(long) ? stateId.ToString() : $"'{stateId}'";
                using var conn = this.config.CreateConnection() as NpgsqlConnection;
                await conn.OpenAsync();
                foreach (var table in getTableListTask.Result.Where(t => t.EndTime >= latestTimestamp))
                {
                    var sql = $"COPY (SELECT typecode,data,version,timestamp from {table.SubTable} WHERE stateid={stateIdStr} and version>={startVersion} and version<={endVersion} order by version asc) TO STDOUT (FORMAT BINARY)";
                    using var reader = conn.BeginBinaryExport(sql);
                    while (reader.StartRow() != -1)
                    {
                        var typeCode = reader.Read<string>(NpgsqlDbType.Varchar);
                        var data = reader.Read<string>(NpgsqlDbType.Json);
                        var version = reader.Read<long>(NpgsqlDbType.Bigint);
                        var timestamp = reader.Read<long>(NpgsqlDbType.Bigint);
                        if (version <= endVersion && version >= startVersion)
                        {
                            if (this.serializer.Deserialize(Encoding.UTF8.GetBytes(data), this.typeFinder.FindType(typeCode)) is IEvent evt)
                            {
                                list.Add(new FullyEvent<PrimaryKey>
                                {
                                    StateId = stateId,
                                    Event = evt,
                                    BasicInfo = new EventBasicInfo(version, timestamp)
                                });
                            }
                        }
                    }
                }
            });
            return list.OrderBy(e => e.BasicInfo.Version).ToList();
        }

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

                var stateIdStr = typeof(PrimaryKey) == typeof(long) ? stateId.ToString() : $"'{stateId.ToString()}'";
                using var conn = this.config.CreateConnection() as NpgsqlConnection;
                await conn.OpenAsync();
                foreach (var table in getTableListTask.Result)
                {
                    var sql = $"COPY (SELECT data,version,timestamp from {table.SubTable} WHERE stateid={stateIdStr} and typecode='{typeCode}' and version>={startVersion} order by version asc limit {limit}) TO STDOUT (FORMAT BINARY)";
                    using var reader = conn.BeginBinaryExport(sql);
                    while (reader.StartRow() != -1)
                    {
                        var data = reader.Read<string>(NpgsqlDbType.Json);
                        var version = reader.Read<long>(NpgsqlDbType.Bigint);
                        var timestamp = reader.Read<long>(NpgsqlDbType.Bigint);
                        if (version >= startVersion && this.serializer.Deserialize(Encoding.UTF8.GetBytes(data), type) is IEvent evt)
                        {
                            list.Add(new FullyEvent<PrimaryKey>
                            {
                                StateId = stateId,
                                Event = evt,
                                BasicInfo = new EventBasicInfo(version, timestamp)
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
                    var copySql = copySaveSqlDict.GetOrAdd(
                        tableName,
                         key => $"copy {key}(stateid,uniqueId,typecode,data,version,timestamp) FROM STDIN (FORMAT BINARY)");
                    using var conn = this.config.CreateConnection() as NpgsqlConnection;
                    await conn.OpenAsync();
                    using var writer = conn.BeginBinaryImport(copySql);
                    foreach (var wrapper in list)
                    {
                        writer.StartRow();
                        writer.Write(wrapper.Value.Event.StateId);
                        writer.Write(wrapper.Value.UniqueId, NpgsqlDbType.Varchar);
                        writer.Write(this.typeFinder.GetCode(wrapper.Value.Event.Event.GetType()), NpgsqlDbType.Varchar);
                        writer.Write(wrapper.Value.EventUtf8String, NpgsqlDbType.Json);
                        writer.Write(wrapper.Value.Event.BasicInfo.Version, NpgsqlDbType.Bigint);
                        writer.Write(wrapper.Value.Event.BasicInfo.Timestamp, NpgsqlDbType.Bigint);
                    }

                    writer.Complete();
                    list.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
                }
                catch (Exception ex)
                {
                    this.logger.LogError(ex, ex.Message);
                    var saveSql = saveSqlDict.GetOrAdd(
                        tableName,
                        key => $"INSERT INTO {key}(stateid,uniqueId,typecode,data,version,timestamp) VALUES(@StateId,@UniqueId,@TypeCode,(@Data)::json,@Version,@Timestamp) ON CONFLICT ON CONSTRAINT {key}_id_unique DO NOTHING");
                    await BatchInsert(saveSql, wrapperList);
                }
            }

            async Task BatchInsert(string saveSql, List<AskInputBox<EventTaskBox<PrimaryKey>, bool>> list)
            {
                bool isSuccess = false;
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

        private static readonly ConcurrentDictionary<string, string> copySaveSqlDict = new ConcurrentDictionary<string, string>();

        public async Task TransactionBatchAppend(List<EventBox<PrimaryKey>> list)
        {
            var minTimestamp = list.Min(t => t.FullyEvent.BasicInfo.Timestamp);
            var maxTimestamp = list.Max(t => t.FullyEvent.BasicInfo.Timestamp);
            var minTask = this.config.GetTable(minTimestamp);
            if (!minTask.IsCompletedSuccessfully)
            {
                await minTask;
            }

            if (minTask.Result.EndTime > maxTimestamp)
            {
                var saveSql = copySaveSqlDict.GetOrAdd(
                    minTask.Result.SubTable,
                    key => $"copy {key}(stateid,uniqueId,typecode,data,version,timestamp) FROM STDIN (FORMAT BINARY)");
                await Task.Run(async () =>
                {
                    using var conn = this.config.CreateConnection() as NpgsqlConnection;
                    await conn.OpenAsync();
                    using var writer = conn.BeginBinaryImport(saveSql);
                    foreach (var wrapper in list)
                    {
                        writer.StartRow();
                        writer.Write(wrapper.FullyEvent.StateId);
                        writer.Write(wrapper.UniqueId, NpgsqlDbType.Varchar);
                        writer.Write(this.typeFinder.GetCode(wrapper.FullyEvent.Event.GetType()), NpgsqlDbType.Varchar);
                        writer.Write(wrapper.EventUtf8String, NpgsqlDbType.Json);
                        writer.Write(wrapper.FullyEvent.BasicInfo.Version, NpgsqlDbType.Bigint);
                        writer.Write(wrapper.FullyEvent.BasicInfo.Timestamp, NpgsqlDbType.Bigint);
                    }

                    writer.Complete();
                });
            }
            else
            {
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
                        var saveSql = saveSqlDict.GetOrAdd(
                            group.Key,
                            key => $"INSERT INTO {key}(stateid,uniqueId,typecode,data,version,timestamp) VALUES(@StateId,@UniqueId,@TypeCode,(@Data)::json,@Version,@Timestamp)");
                        await conn.ExecuteAsync(saveSql, group.Select(g => new
                        {
                            g.t.FullyEvent.StateId,
                            g.t.UniqueId,
                            TypeCode = this.typeFinder.GetCode(g.t.FullyEvent.Event.GetType()),
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
                    this.logger.LogError(ex, nameof(this.TransactionBatchAppend));
                    throw;
                }
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
                var getTableListTask = this.config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                {
                    await getTableListTask;
                }

                var table = getTableListTask.Result.SingleOrDefault(t => t.StartTime <= timestamp && t.EndTime >= timestamp);
                using var conn = this.config.CreateConnection();
                var sql = $"delete from {table.SubTable} WHERE stateid=@StateId and version=@Version";
                await conn.ExecuteAsync(sql, new { StateId = stateId, Version = version });
            });
        }
    }
}
