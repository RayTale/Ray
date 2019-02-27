using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class ArchiveStorage<PrimaryKey, StateType> : IArchiveStorage<PrimaryKey, StateType>
          where StateType : class, new()
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
        readonly ISerializer serializer;
        public ArchiveStorage(ISerializer serializer, StorageConfig table)
        {
            this.serializer = serializer;
            tableInfo = table;
            var tableName = table.ArchiveSnapshotTable;
            deleteSql = $"DELETE FROM {tableName} where id=@Id";
            deleteAllSql = $"DELETE FROM {tableName} where stateid=@StateId";
            getByIdSql = $"select * FROM {tableName} where id=@Id";
            getListByStateIdSql = $"select Id,StartVersion,EndVersion,StartTimestamp,EndTimestamp,Index,EventIsCleared FROM {tableName} where stateid=@StateId";
            getLatestByStateIdSql = $"select Id,StartVersion,EndVersion,StartTimestamp,EndTimestamp,Index,EventIsCleared FROM {tableName} where stateid=@StateId order by index desc limit 1";
            insertSql = $"INSERT into {tableName}(Id,stateid,StartVersion,EndVersion,StartTimestamp,EndTimestamp,Index,EventIsCleared,data,IsOver,Version)VALUES(@Id,@StateId,@StartVersion,@EndVersion,@StartTimestamp,@EndTimestamp,@Index,@EventIsCleared,(@Data)::jsonb,@IsOver,@Version)";
            updateOverSql = $"update {tableName} set IsOver=@IsOver where stateid=@StateId";
            updateEventIsClearSql = $"update {tableName} set EventIsCleared=true where id=@Id";
        }
        public async Task Delete(PrimaryKey stateId, string briefId)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                await conn.ExecuteAsync(deleteSql, new { Id = briefId });
            }
        }
        public async Task DeleteAll(PrimaryKey stateId)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                await conn.ExecuteAsync(deleteAllSql, new { StateId = stateId.ToString() });
            }
        }
        public async Task EventIsClear(PrimaryKey stateId, string briefId)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateEventIsClearSql, new { Id = briefId });
            }
        }

        public async Task<List<ArchiveBrief>> GetBriefList(PrimaryKey stateId)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                return (await connection.QueryAsync<ArchiveBrief>(getListByStateIdSql, new { StateId = stateId.ToString() })).AsList();
            }
        }

        public async Task<ArchiveBrief> GetLatestBrief(PrimaryKey stateId)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                return await connection.QuerySingleOrDefaultAsync<ArchiveBrief>(getLatestByStateIdSql, new { StateId = stateId.ToString() });
            }
        }

        public async Task<Snapshot<PrimaryKey, StateType>> GetById(string briefId)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                var data = await connection.QuerySingleOrDefaultAsync<StateModel>(getByIdSql, new { Id = briefId });
                if (data != default)
                {
                    return new Snapshot<PrimaryKey, StateType>()
                    {
                        Base = new SnapshotBase<PrimaryKey>
                        {
                            StateId = serializer.Deserialize<PrimaryKey>(data.StateId),
                            Version = data.Version,
                            DoingVersion = data.Version,
                            IsLatest = false,
                            IsOver = data.IsOver,
                            StartTimestamp = data.StartTimestamp,
                            LatestMinEventTimestamp = data.StartTimestamp
                        },
                        State = serializer.Deserialize<StateType>(data.Data)
                    };
                }
            }
            return default;
        }

        public async Task Insert(ArchiveBrief brief, Snapshot<PrimaryKey, StateType> snapshot)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(insertSql, new
                {
                    brief.Id,
                    snapshot.Base.StateId,
                    brief.StartVersion,
                    brief.EndVersion,
                    brief.StartTimestamp,
                    brief.EndTimestamp,
                    brief.Index,
                    brief.EventIsCleared,
                    Data = serializer.SerializeToString(snapshot.State),
                    snapshot.Base.IsOver,
                    snapshot.Base.Version
                });
            }
        }
        public async Task Over(PrimaryKey stateId, bool isOver)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateOverSql, new { StateId = stateId.ToString(), IsOver = isOver });
            }
        }
    }
}
