using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;
using Ray.Core.Serialization;
using Ray.Core.State;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class ArchiveStorage<K, S> : IArchiveStorage<K, S>
          where S : class, new()
    {
        readonly StorageConfig tableInfo;
        private readonly string deleteSql;
        private readonly string deleteAllSql;
        private readonly string getByIdSql;
        private readonly string getListByStateIdSql;
        private readonly string getLatestByStateIdSql;
        private readonly string insertSql;
        private readonly string updateOverSql;
        private readonly string updateEventIsClearSql;
        readonly IJsonSerializer serializer;
        public ArchiveStorage(IJsonSerializer serializer, StorageConfig table)
        {
            this.serializer = serializer;
            tableInfo = table;
            var tableName = table.ArchiveStateTable;
            deleteSql = $"DELETE FROM {tableName} where id=@Id";
            deleteAllSql = $"DELETE FROM {tableName} where stateid=@StateId";
            getByIdSql = $"select * FROM {tableName} where id=@Id";
            getListByStateIdSql = $"select Id,StartVersion,EndVersion,StartTimestamp,EndTimestamp,Index,EventIsCleared FROM {tableName} where stateid=@StateId";
            getLatestByStateIdSql = $"select Id,StartVersion,EndVersion,StartTimestamp,EndTimestamp,Index,EventIsCleared FROM {tableName} where stateid=@StateId order by index desc limit 1";
            insertSql = $"INSERT into {tableName}(Id,stateid,StartVersion,EndVersion,StartTimestamp,EndTimestamp,Index,EventIsCleared,data,IsOver,Version)VALUES(@Id,@StateId,@StartVersion,@EndVersion,@StartTimestamp,@EndTimestamp,@Index,@EventIsCleared,(@Data)::jsonb,@IsOver,@Version)";
            updateOverSql = $"update {tableName} set IsOver=@IsOver where stateid=@StateId";
            updateEventIsClearSql = $"update {tableName} set EventIsCleared=true where id=@Id";
        }
        public async Task Delete(string briefId)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                await conn.ExecuteAsync(deleteSql, new { Id = briefId });
            }
        }
        public async Task DeleteAll(K stateId)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                await conn.ExecuteAsync(deleteAllSql, new { StateId = stateId.ToString() });
            }
        }
        public async Task EventIsClear(string briefId)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateEventIsClearSql, new { Id = briefId });
            }
        }

        public async Task<List<ArchiveBrief>> GetBriefList(K stateId)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                return (await connection.QueryAsync<ArchiveBrief>(getListByStateIdSql, new { StateId = stateId.ToString() })).AsList();
            }
        }

        public async Task<ArchiveBrief> GetLatestBrief(K stateId)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                return await connection.QuerySingleOrDefaultAsync<ArchiveBrief>(getLatestByStateIdSql, new { StateId = stateId.ToString() });
            }
        }

        public async Task<Snapshot<K, S>> GetState(string briefId)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                var data = await connection.QuerySingleOrDefaultAsync<StateModel>(getByIdSql, new { Id = briefId });
                if (data != default)
                {
                    return new Snapshot<K, S>()
                    {
                        Base = new SnapshotBase<K>
                        {
                            StateId = serializer.Deserialize<K>(data.StateId),
                            Version = data.Version,
                            DoingVersion = data.Version,
                            IsLatest = false,
                            IsOver = data.IsOver,
                            StartTimestamp = data.StartTimestamp,
                            LatestMinEventTimestamp = data.StartTimestamp
                        },
                        State = serializer.Deserialize<S>(data.Data)
                    };
                }
            }
            return default;
        }

        public async Task Insert(ArchiveBrief brief, Snapshot<K, S> state)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(insertSql, new
                {
                    brief.Id,
                    state.Base.StateId,
                    brief.StartVersion,
                    brief.EndVersion,
                    brief.StartTimestamp,
                    brief.EndTimestamp,
                    brief.Index,
                    brief.EventIsCleared,
                    Data = serializer.Serialize(state.State),
                    state.Base.IsOver,
                    state.Base.Version
                });
            }
        }
        public async Task Over(K stateId, bool isOver)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateOverSql, new { StateId = stateId.ToString(), IsOver = isOver });
            }
        }
    }
}
