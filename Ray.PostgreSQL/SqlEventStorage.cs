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
using ProtoBuf;
using Ray.Core.Abstractions;
using Ray.Core.Internal;
using Ray.Core.Messaging;

namespace Ray.Storage.PostgreSQL
{
    public class SqlEventStorage<K> : IEventStorage<K>
    {
        readonly SqlGrainConfig tableInfo;
        readonly IMpscChannel<BytesEventTaskSource<K>> mpscChannel;
        readonly ILogger<SqlEventStorage<K>> logger;
        public SqlEventStorage(IServiceProvider serviceProvider, SqlGrainConfig tableInfo)
        {
            logger = serviceProvider.GetService<ILogger<SqlEventStorage<K>>>();
            mpscChannel = serviceProvider.GetService<IMpscChannel<BytesEventTaskSource<K>>>().BindConsumer(BatchProcessing);
            mpscChannel.ActiveConsumer();
            this.tableInfo = tableInfo;
        }
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, long startVersion, long endVersion)
        {
            var originList = new List<SqlEvent>((int)(endVersion - startVersion));
            await Task.Run(async () =>
            {
                var getTableListTask = tableInfo.GetTableListFromDb();
                if (!getTableListTask.IsCompleted)
                    await getTableListTask;
                var tableList = getTableListTask.Result;
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
                                originList.Add(new SqlEvent { TypeCode = reader.Read<string>(NpgsqlDbType.Varchar), Data = reader.Read<byte[]>(NpgsqlDbType.Bytea) });
                            }
                        }
                    }
                }
            });

            var list = new List<IEventBase<K>>(originList.Count);
            foreach (var origin in originList)
            {
                using (var ms = new MemoryStream(origin.Data))
                {
                    if (Serializer.Deserialize(TypeContainer.GetType(origin.TypeCode), ms) is IEventBase<K> evt)
                    {
                        list.Add(evt);
                    }
                }
            }
            return list.OrderBy(v => v.Version).ToList();
        }
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, string typeCode, long startVersion, int limit)
        {
            var originList = new List<byte[]>(limit);
            var type = TypeContainer.GetType(typeCode);
            await Task.Run(async () =>
            {
                var getTableListTask = tableInfo.GetTableListFromDb();
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
            var list = new List<IEventBase<K>>(originList.Count);
            foreach (var origin in originList)
            {
                using (var ms = new MemoryStream(origin))
                {
                    if (Serializer.Deserialize(type, ms) is IEventBase<K> evt)
                    {
                        list.Add(evt);
                    }
                }
            }
            return list.OrderBy(v => v.Version).ToList();
        }

        static readonly ConcurrentDictionary<string, string> saveSqlDict = new ConcurrentDictionary<string, string>();
        public Task<bool> SaveAsync(IEventBase<K> evt, byte[] bytes, string uniqueId = null)
        {
            return Task.Run(async () =>
            {
                var wrap = new BytesEventTaskSource<K>(evt, bytes, uniqueId);
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompleted)
                    await writeTask;
                return await wrap.TaskSource.Task;
            });
        }
        private async Task BatchProcessing(List<BytesEventTaskSource<K>> wrapList)
        {
            var copySql = copySaveSqlDict.GetOrAdd((await tableInfo.GetTable(DateTime.UtcNow)).Name, key => $"copy {key}(stateid,uniqueId,typecode,data,version) FROM STDIN (FORMAT BINARY)");
            try
            {
                using (var conn = tableInfo.CreateConnection() as NpgsqlConnection)
                {
                    await conn.OpenAsync();
                    using (var writer = conn.BeginBinaryImport(copySql))
                    {
                        foreach (var evt in wrapList)
                        {
                            writer.StartRow();
                            writer.Write(evt.Value.StateId.ToString(), NpgsqlDbType.Varchar);
                            writer.Write(evt.UniqueId, NpgsqlDbType.Varchar);
                            writer.Write(evt.Value.GetType().FullName, NpgsqlDbType.Varchar);
                            writer.Write(evt.Bytes, NpgsqlDbType.Bytea);
                            writer.Write(evt.Value.Version, NpgsqlDbType.Bigint);
                        }
                        writer.Complete();
                    }
                }
                wrapList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
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
                            foreach (var w in wrapList)
                            {
                                w.Result = await conn.ExecuteAsync(saveSql, new { StateId = w.Value.StateId.ToString(), w.UniqueId, w.Value.GetType().FullName, Data = w.Bytes, w.Value.Version }, trans) > 0;
                            }
                            trans.Commit();
                            wrapList.ForEach(wrap => wrap.TaskSource.TrySetResult(wrap.Result));
                        }
                        catch (Exception e)
                        {
                            trans.Rollback();
                            wrapList.ForEach(wrap => wrap.TaskSource.TrySetException(e));
                        }
                    }
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask<string> GetInsertSql()
        {
            return saveSqlDict.GetOrAdd((await tableInfo.GetTable(DateTime.UtcNow)).Name,
                key => $"INSERT INTO {key}(stateid,uniqueId,typecode,data,version) VALUES(@StateId,@UniqueId,@TypeCode,@Data,@Version) ON CONFLICT ON CONSTRAINT {key}_id_unique DO NOTHING");
        }
        static readonly ConcurrentDictionary<string, string> copySaveSqlDict = new ConcurrentDictionary<string, string>();
        public async Task TransactionSaveAsync(List<TransactionEventWrapper<K>> list)
        {
            var getTableTask = tableInfo.GetTable(DateTime.UtcNow);
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
                        foreach (var evt in list)
                        {
                            writer.StartRow();
                            writer.Write(evt.Evt.StateId.ToString(), NpgsqlDbType.Varchar);
                            writer.Write(evt.UniqueId, NpgsqlDbType.Varchar);
                            writer.Write(evt.Evt.GetType().FullName, NpgsqlDbType.Varchar);
                            writer.Write(evt.Bytes, NpgsqlDbType.Bytea);
                            writer.Write(evt.Evt.Version, NpgsqlDbType.Bigint);
                        }
                        writer.Complete();
                    }
                }
            });
        }
    }
}
