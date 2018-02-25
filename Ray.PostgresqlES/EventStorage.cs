using Ray.Core.EventSourcing;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Dapper;
using Ray.Core.Message;
using System.IO;
using System.Linq;
using ProtoBuf;
using Ray.Core.Utils;

namespace Ray.PostgresqlES
{
    public class EventStorage<K> : IEventStorage<K>
    {
        SqlTable tableInfo;
        public EventStorage(SqlTable mongoAttr)
        {
            this.tableInfo = mongoAttr;
        }
        static ConcurrentDictionary<string, string> completeSqlDict = new ConcurrentDictionary<string, string>();
        public async Task CompleteAsync<T>(T data) where T : IEventBase<K>
        {
            var table = await tableInfo.GetTable(data.Timestamp);
            if (!completeSqlDict.TryGetValue(table.Name, out var sql))
            {
                sql = $"UPDATE {table.Name} set iscomplete=TRUE where id=@Id";
                completeSqlDict.TryAdd(table.Name, sql);
            }
            using (var conn = tableInfo.CreateConnection())
            {
                await conn.ExecuteAsync(sql, new { Id = data.Id });
            }
        }

        public async Task<List<EventInfo<K>>> GetListAsync(K stateId, Int64 startVersion, Int64 endVersion, DateTime? startTime = null)
        {
            var tableList = await tableInfo.GetTableList(startTime);
            var list = new List<EventInfo<K>>();
            Int64 readVersion = 0;
            using (var conn = tableInfo.CreateConnection())
            {
                foreach (var table in tableList)
                {
                    var sql = $"SELECT typecode,data,IsComplete from {table.Name} WHERE stateid=@StateId and version>@Start and version<=@End";

                    var sqlEventList = await conn.QueryAsync<SqlEvent>(sql, new { StateId = stateId, Start = startVersion, End = endVersion });
                    foreach (var sqlEvent in sqlEventList)
                    {
                        var type = MessageTypeMapping.GetType(sqlEvent.TypeCode);
                        var eventInfo = new EventInfo<K>();
                        eventInfo.IsComplete = sqlEvent.IsComplete;
                        using (var ms = new MemoryStream(sqlEvent.Data))
                        {
                            var @event = Serializer.Deserialize(type, ms) as IEventBase<K>;
                            readVersion = @event.Version;
                            eventInfo.Event = @event;
                        }
                        if (readVersion <= endVersion)
                            list.Add(eventInfo);
                    }
                    if (readVersion >= endVersion)
                        break;
                }
            }
            return list.OrderBy(e => e.Event.Version).ToList();
        }

        public async Task<List<EventInfo<K>>> GetListAsync(K stateId, string typeCode, Int64 startVersion, Int64 endVersion, DateTime? startTime = null)
        {
            var tableList = await tableInfo.GetTableList(startTime);
            var list = new List<EventInfo<K>>();
            Int64 readVersion = 0;
            using (var conn = tableInfo.CreateConnection())
            {
                foreach (var table in tableList)
                {
                    var sql = $"SELECT typecode,data,IsComplete from {table.Name} WHERE stateid=@StateId and typecode=@TypeCode and version>@Start and version<=@End";

                    var sqlEventList = await conn.QueryAsync<SqlEvent>(sql, new { StateId = stateId, TypeCode = typeCode, Start = startVersion, End = endVersion });
                    foreach (var sqlEvent in sqlEventList)
                    {
                        var type = MessageTypeMapping.GetType(sqlEvent.TypeCode);
                        var eventInfo = new EventInfo<K>();
                        eventInfo.IsComplete = sqlEvent.IsComplete;
                        using (var ms = new MemoryStream(sqlEvent.Data))
                        {
                            var @event = Serializer.Deserialize(type, ms) as IEventBase<K>;
                            readVersion = @event.Version;
                            eventInfo.Event = @event;
                        }
                        if (readVersion <= endVersion)
                            list.Add(eventInfo);
                    }
                    if (readVersion >= endVersion)
                        break;
                }
            }
            return list.OrderBy(e => e.Event.Version).ToList();
        }

        public async Task<bool> SaveAsync<T>(T data, byte[] bytes, string uniqueId = null) where T : IEventBase<K>
        {
            var table = await tableInfo.GetTable(data.Timestamp);
            var saveSql = $"INSERT INTO {table.Name}(Id,stateid,msgid,typecode,data,version,iscomplete) VALUES(@Id,@StateId,@MsgId,@TypeCode,@Data,@Version,@IsComplete)";
            data.Id = OGuid.GenerateNewId().ToString();
            string msgId = uniqueId;
            if (string.IsNullOrEmpty(msgId))
                msgId = data.Id;
            try
            {
                using (var conn = tableInfo.CreateConnection())
                {
                    await conn.ExecuteAsync(saveSql, new { data.Id, StateId = data.StateId.ToString(), MsgId = msgId, data.TypeCode, Data = bytes, Version = data.Version, IsComplete = false });
                }
                return true;
            }
            catch (Exception ex)
            {
                if (!(ex.InnerException is Npgsql.PostgresException e && e.SqlState == "23505"))
                {
                    throw ex;
                }
            }
            return false;
        }
    }
}
