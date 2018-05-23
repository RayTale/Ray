using Dapper;
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
        ConcurrentDictionary<string, string> oneListSqlDict = new ConcurrentDictionary<string, string>();
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, Int64 startVersion, Int64 endVersion, DateTime? startTime = null)
        {
            var tableList = await tableInfo.GetTableList(startTime);
            var list = new List<IEventBase<K>>();
            Int64 readVersion = 0;
            using (var conn = tableInfo.CreateConnection())
            {
                foreach (var table in tableList)
                {
                    if (!oneListSqlDict.TryGetValue(table.Name, out var sql))
                    {
                        sql = $"SELECT typecode,data from {table.Name} WHERE stateid=@StateId and version>@Start and version<=@End order by version asc";
                        oneListSqlDict.TryAdd(table.Name, sql);
                    }
                    var sqlEventList = await conn.QueryAsync<SqlEvent>(sql, new { StateId = stateId.ToString(), Start = startVersion, End = endVersion });
                    foreach (var sqlEvent in sqlEventList)
                    {
                        var type = MessageTypeMapper.GetType(sqlEvent.TypeCode);
                        using (var ms = new MemoryStream(sqlEvent.Data))
                        {
                            if (Serializer.Deserialize(type, ms) is IEventBase<K> evt)
                            {
                                readVersion = evt.Version;
                                if (readVersion <= endVersion)
                                    list.Add(evt);
                            }
                        }
                    }
                    if (readVersion >= endVersion)
                        break;
                }
            }
            return list;
        }
        ConcurrentDictionary<string, string> twoListSqlDict = new ConcurrentDictionary<string, string>();
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, string typeCode, Int64 startVersion, Int32 limit, DateTime? startTime = null)
        {
            var tableList = await tableInfo.GetTableList(startTime);
            var list = new List<IEventBase<K>>();
            using (var conn = tableInfo.CreateConnection())
            {
                foreach (var table in tableList)
                {
                    if (!twoListSqlDict.TryGetValue(table.Name, out var sql))
                    {
                        sql = $"SELECT typecode,data from {table.Name} WHERE stateid=@StateId and typecode=@TypeCode and version>@Start order by version asc limit @Limit";
                        twoListSqlDict.TryAdd(table.Name, sql);
                    }
                    var sqlEventList = await conn.QueryAsync<SqlEvent>(sql, new { StateId = stateId.ToString(), TypeCode = typeCode, Start = startVersion, Limit = limit - list.Count });
                    foreach (var sqlEvent in sqlEventList)
                    {
                        var type = MessageTypeMapper.GetType(sqlEvent.TypeCode);
                        using (var ms = new MemoryStream(sqlEvent.Data))
                        {
                            if (Serializer.Deserialize(type, ms) is IEventBase<K> evt)
                            {
                                list.Add(evt);
                            }
                        }
                    }
                    if (list.Count >= limit)
                        break;
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
                if (!(ex is Npgsql.PostgresException e && e.SqlState == "23505"))
                {
                    throw ex;
                }
            }
            return false;
        }
    }
}
