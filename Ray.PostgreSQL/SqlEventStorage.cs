using Dapper;
using Npgsql;
using NpgsqlTypes;
using ProtoBuf;
using Ray.Core.EventSourcing;
using Ray.Core.Message;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Ray.PostgreSQL
{
    public class SqlEventStorage<K> : IEventStorage<K>
    {
        SqlGrainConfig tableInfo;
        public SqlEventStorage(SqlGrainConfig tableInfo)
        {
            this.tableInfo = tableInfo;
        }
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, Int64 startVersion, Int64 endVersion, DateTime? startTime = null)
        {
            var originList = new List<SqlEvent>((int)(endVersion - startVersion));
            await Task.Run(async () =>
            {
                var tableList = await tableInfo.GetTableList(startTime);
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
            }).ConfigureAwait(false);

            var list = new List<IEventBase<K>>(originList.Count);
            foreach (var origin in originList)
            {
                if (MessageTypeMapper.EventTypeDict.TryGetValue(origin.TypeCode, out var type))
                {
                    using (var ms = new MemoryStream(origin.Data))
                    {
                        if (Serializer.Deserialize(type, ms) is IEventBase<K> evt)
                        {
                            list.Add(evt);
                        }
                    }
                }
            }
            return list;
        }
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, string typeCode, Int64 startVersion, Int32 limit, DateTime? startTime = null)
        {
            var originList = new List<byte[]>(limit);
            if (MessageTypeMapper.EventTypeDict.TryGetValue(typeCode, out var type))
            {
                await Task.Run(async () =>
                {
                    var tableList = await tableInfo.GetTableList(startTime);
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
                }).ConfigureAwait(false);
            }
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
            return list;
        }
        ConcurrentDictionary<string, string> saveSqlDict = new ConcurrentDictionary<string, string>();
        public async Task<bool> SaveAsync(IEventBase<K> evt, byte[] bytes, string uniqueId = null)
        {
            var table = await tableInfo.GetTable(evt.Timestamp);
            if (!saveSqlDict.TryGetValue(table.Name, out var saveSql))
            {
                saveSql = $"INSERT INTO {table.Name}(stateid,uniqueId,typecode,data,version) VALUES(@StateId,@UniqueId,@TypeCode,@Data,@Version)";
                saveSqlDict.TryAdd(table.Name, saveSql);
            }
            if (string.IsNullOrEmpty(uniqueId))
                uniqueId = evt.GetUniqueId();
            try
            {
                using (var conn = tableInfo.CreateConnection())
                {
                    await conn.ExecuteAsync(saveSql, new { StateId = evt.StateId.ToString(), UniqueId = uniqueId, evt.TypeCode, Data = bytes, evt.Version });
                }
                return true;
            }
            catch (Exception ex)
            {
                if (!(ex is PostgresException e && e.SqlState == "23505"))
                {
                    throw ex;
                }
            }
            return false;
        }
    }
}
