using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;

namespace Ray.Storage.PostgreSQL
{
    public class TableRepository
    {
        readonly StorageConfig storageConfig;
        public TableRepository(StorageConfig storageConfig)
        {
            this.storageConfig = storageConfig;
        }
        public async Task<List<TableInfo>> GetTableListFromDb()
        {
            const string sql = "SELECT * FROM ray_tablelist where prefix=@Table order by version asc";
            using (var connection = SqlFactory.CreateConnection(storageConfig.Connection))
            {
                return (await connection.QueryAsync<TableInfo>(sql, new { Table = storageConfig.EventTable })).AsList();
            }
        }
        static readonly ConcurrentDictionary<string, bool> createEventTableListDict = new ConcurrentDictionary<string, bool>();
        static readonly ConcurrentQueue<TaskCompletionSource<bool>> createEventTableListTaskList = new ConcurrentQueue<TaskCompletionSource<bool>>();
        public async Task CreateEventTable(TableInfo table)
        {
            const string sql = @"
                    create table {0} (
                            StateId varchar({1}) not null,
                            UniqueId varchar(250)  null,
                            TypeCode varchar(100)  not null,
                            Data bytea not null,
                            Version int8 not null,
                            Timestamp int8 not null,
                            constraint {0}_id_unique unique(StateId,TypeCode,UniqueId)
                            ) WITH (OIDS=FALSE);
                            CREATE UNIQUE INDEX {0}_Version ON {0} USING btree(StateId, Version);";
            const string insertSql = "INSERT into ray_tablelist  VALUES(@Prefix,@Name,@Version,@CreateTime)";
            var key = $"{storageConfig.Connection}-{table.Name}-{storageConfig.StateIdLength}";
            if (createEventTableListDict.TryAdd(key, true))
            {
                using (var connection = SqlFactory.CreateConnection(storageConfig.Connection))
                {
                    await connection.OpenAsync();
                    using (var trans = connection.BeginTransaction())
                    {
                        try
                        {
                            await connection.ExecuteAsync(string.Format(sql, table.Name, storageConfig.StateIdLength), transaction: trans);
                            await connection.ExecuteAsync(insertSql, table, trans);
                            trans.Commit();
                            createEventTableListDict[key] = true;
                        }
                        catch (Exception e)
                        {
                            trans.Rollback();
                            if (e is Npgsql.PostgresException ne && ne.ErrorCode == -2147467259)
                                return;
                            else
                            {
                                createEventTableListDict.TryRemove(key, out var v);
                                throw;
                            }
                        }
                    }
                }
                while (true)
                {
                    if (createEventTableListTaskList.TryDequeue(out var task))
                    {
                        task.TrySetResult(true);
                    }
                    else
                        break;
                }
            }
            else if (createEventTableListDict[key] == false)
            {
                var task = new TaskCompletionSource<bool>();
                createEventTableListTaskList.Enqueue(task);
                try
                {
                    await task.Task;
                }
                catch
                {
                    await CreateEventTable(table);
                }
            }
        }
        static readonly ConcurrentDictionary<string, bool> createStateTableDict = new ConcurrentDictionary<string, bool>();
        static readonly ConcurrentQueue<TaskCompletionSource<bool>> createStateTableTaskList = new ConcurrentQueue<TaskCompletionSource<bool>>();
        public async Task CreateStateTable()
        {
            const string sql = @"
                     CREATE TABLE if not exists {0}(
                     StateId varchar({1}) not null PRIMARY KEY,
                     Data jsonb not null,
                     Version int8 not null,
                     StartTimestamp int8 not null,
                     LatestMinEventTimestamp int8 not null,
                     IsLatest bool not null,
                     IsOver bool not null)";
            var key = $"{storageConfig.Connection}-{storageConfig.SnapshotTable}-{storageConfig.StateIdLength}";
            if (createStateTableDict.TryAdd(key, false))
            {
                using (var connection = SqlFactory.CreateConnection(storageConfig.Connection))
                {
                    try
                    {
                        await connection.ExecuteAsync(string.Format(sql, storageConfig.SnapshotTable, storageConfig.StateIdLength));
                        createStateTableDict[key] = true;
                    }
                    catch (Exception e)
                    {
                        if (e is Npgsql.PostgresException ne && ne.ErrorCode == -2147467259)
                            return;
                        else
                        {
                            createStateTableDict.TryRemove(key, out var v);
                            throw;
                        }
                    }
                }
                while (true)
                {
                    if (createStateTableTaskList.TryDequeue(out var task))
                    {
                        task.TrySetResult(true);
                    }
                    else
                        break;
                }
            }
            else if (createStateTableDict[key] == false)
            {
                var task = new TaskCompletionSource<bool>();
                createStateTableTaskList.Enqueue(task);
                try
                {
                    await task.Task;
                }
                catch
                {
                    await CreateStateTable();
                }
            }
        }
        static readonly ConcurrentDictionary<string, bool> createFollowStateTableDict = new ConcurrentDictionary<string, bool>();
        static readonly ConcurrentQueue<TaskCompletionSource<bool>> createFollowStateTableTaskList = new ConcurrentQueue<TaskCompletionSource<bool>>();
        public async Task CreateFollowStateTable()
        {
            const string sql = @"
                     CREATE TABLE if not exists {0}(
                     StateId varchar({1}) not null PRIMARY KEY,
                     DoingVersion int8 not null,
                     Version int8 not null)";
            var key = $"{storageConfig.Connection}-{storageConfig.GetFollowStateTable()}-{storageConfig.StateIdLength}";
            if (createFollowStateTableDict.TryAdd(key, false))
            {
                using (var connection = SqlFactory.CreateConnection(storageConfig.Connection))
                {
                    try
                    {
                        await connection.ExecuteAsync(string.Format(sql, $"{storageConfig.SnapshotTable}_{storageConfig.FollowName}", storageConfig.StateIdLength));
                        createFollowStateTableDict[key] = true;
                    }
                    catch (Exception e)
                    {
                        if (e is Npgsql.PostgresException ne && ne.ErrorCode == -2147467259)
                            return;
                        else
                        {
                            createFollowStateTableDict.TryRemove(key, out var v);
                            throw;
                        }
                    }
                }
                while (true)
                {
                    if (createFollowStateTableTaskList.TryDequeue(out var task))
                    {
                        task.TrySetResult(true);
                    }
                    else
                        break;
                }
            }
            else if (createFollowStateTableDict[key] == false)
            {
                var task = new TaskCompletionSource<bool>();
                createFollowStateTableTaskList.Enqueue(task);
                try
                {
                    await task.Task;
                }
                catch
                {
                    await CreateStateTable();
                }
            }
        }
        private readonly ConcurrentDictionary<string, bool> createArchiveStateTableDict = new ConcurrentDictionary<string, bool>();
        private readonly ConcurrentQueue<TaskCompletionSource<bool>> createArchiveStateTableTaskList = new ConcurrentQueue<TaskCompletionSource<bool>>();
        public async Task CreateArchiveStateTable()
        {
            const string sql = @"
                     CREATE TABLE if not exists {0}(
                     Id varchar(50) not null PRIMARY KEY,
                     StateId varchar({1}) not null,
                     StartVersion int8 not null,
                     EndVersion int8 not null,
                     StartTimestamp int8 not null,
                     EndTimestamp int8 not null,
                     Index int4 not null,
                     EventIsCleared bool not null,
                     Data jsonb not null,
                     IsOver bool not null,
                     Version int8 not null)WITH (OIDS=FALSE);
                     CREATE INDEX {0}_Archive ON {0} USING btree(StateId)";
            var key = $"{storageConfig.Connection}-{storageConfig.SnapshotTable}-{storageConfig.StateIdLength}";
            if (createArchiveStateTableDict.TryAdd(key, false))
            {
                using (var connection = SqlFactory.CreateConnection(storageConfig.Connection))
                {
                    try
                    {
                        await connection.ExecuteAsync(string.Format(sql, storageConfig.ArchiveStateTable, storageConfig.StateIdLength));
                        createArchiveStateTableDict[key] = true;
                    }
                    catch (Exception e)
                    {
                        if (e is Npgsql.PostgresException ne && ne.ErrorCode == -2147467259)
                            return;
                        else
                        {
                            createArchiveStateTableDict.TryRemove(key, out var v);
                            throw;
                        }
                    }
                }
                while (true)
                {
                    if (createArchiveStateTableTaskList.TryDequeue(out var task))
                    {
                        task.TrySetResult(true);
                    }
                    else
                        break;
                }
            }
            else if (createArchiveStateTableDict[key] == false)
            {
                var task = new TaskCompletionSource<bool>();
                createArchiveStateTableTaskList.Enqueue(task);
                try
                {
                    await task.Task;
                }
                catch
                {
                    await CreateArchiveStateTable();
                }
            }
        }
        readonly ConcurrentDictionary<string, bool> createTableListDict = new ConcurrentDictionary<string, bool>();
        readonly ConcurrentQueue<TaskCompletionSource<bool>> createTableListTaskList = new ConcurrentQueue<TaskCompletionSource<bool>>();
        public async Task CreateSubRecordTable()
        {
            const string sql = @"
                    CREATE TABLE IF Not EXISTS ray_tablelist(
                        Prefix varchar(255) not null,
                        Name varchar(255) not null,
                        Version int4,
                        Createtime int8
                    )WITH (OIDS=FALSE);
                    CREATE UNIQUE INDEX IF NOT EXISTS table_version ON ray_tablelist USING btree(Prefix, Version)";
            if (createTableListDict.TryAdd(storageConfig.Connection, true))
            {
                using (var connection = SqlFactory.CreateConnection(storageConfig.Connection))
                {
                    try
                    {
                        await connection.ExecuteAsync(sql);
                        createTableListDict[storageConfig.Connection] = true;
                    }
                    catch (Exception e)
                    {
                        if (e is Npgsql.PostgresException pe && pe.ErrorCode == -2147467259)
                            return;
                        else
                        {
                            createTableListDict.TryRemove(storageConfig.Connection, out var v);
                            throw;
                        }
                    }
                }
                while (true)
                {
                    if (createTableListTaskList.TryDequeue(out var task))
                    {
                        task.TrySetResult(true);
                    }
                    else
                        break;
                }
            }
            else if (createTableListDict[storageConfig.Connection] == false)
            {
                var task = new TaskCompletionSource<bool>();
                createTableListTaskList.Enqueue(task);
                try
                {
                    await task.Task;
                }
                catch
                {
                    await CreateSubRecordTable();
                }
            }
        }
    }
}
