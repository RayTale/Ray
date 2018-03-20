using Ray.Core.EventSourcing;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;
using Ray.Core.Message;
using System.IO;
using ProtoBuf;
using Ray.Core.Utils;

namespace Ray.Postgresql
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
            var tableList = await tableInfo.GetTableList(startTime);
            var list = new List<IEventBase<K>>();
            Int64 readVersion = 0;
            using (var conn = tableInfo.CreateConnection())
            {
                foreach (var table in tableList)
                {
                    var sql = $"SELECT typecode,data from {table.Name} WHERE stateid=@StateId and version>@Start and version<=@End order by version asc";

                    var sqlEventList = await conn.QueryAsync<SqlEvent>(sql, new { StateId = stateId, Start = startVersion, End = endVersion });
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

        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, string typeCode, Int64 startVersion, Int64 endVersion, DateTime? startTime = null)
        {
            var tableList = await tableInfo.GetTableList(startTime);
            var list = new List<IEventBase<K>>();
            Int64 readVersion = 0;
            using (var conn = tableInfo.CreateConnection())
            {
                foreach (var table in tableList)
                {
                    var sql = $"SELECT typecode,data from {table.Name} WHERE stateid=@StateId and typecode=@TypeCode and version>@Start and version<=@End order by version asc";

                    var sqlEventList = await conn.QueryAsync<SqlEvent>(sql, new { StateId = stateId, TypeCode = typeCode, Start = startVersion, End = endVersion });
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

        public async Task<bool> SaveAsync(IEventBase<K> data, byte[] bytes, string uniqueId = null) 
        {
            var table = await tableInfo.GetTable(data.Timestamp);
            var saveSql = $"INSERT INTO {table.Name}(Id,stateid,msgid,typecode,data,version) VALUES(@Id,@StateId,@MsgId,@TypeCode,@Data,@Version)";
            data.Id = OGuid.GenerateNewId().ToString();
            string msgId = uniqueId;
            if (string.IsNullOrEmpty(msgId))
                msgId = data.Id;
            try
            {
                using (var conn = tableInfo.CreateConnection())
                {
                    await conn.ExecuteAsync(saveSql, new { data.Id, StateId = data.StateId.ToString(), MsgId = msgId, data.TypeCode, Data = bytes, data.Version });
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
