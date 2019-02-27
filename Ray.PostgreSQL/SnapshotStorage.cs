using System.Threading.Tasks;
using Dapper;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class SnapshotStorage<PrimaryKey, StateType> : ISnapshotStorage<PrimaryKey, StateType>
        where StateType : class, new()
    {
        readonly StorageConfig tableInfo;
        private readonly string deleteSql;
        private readonly string getByIdSql;
        private readonly string insertSql;
        private readonly string updateSql;
        private readonly string updateOverSql;
        private readonly string updateIsLatestSql;
        private readonly string updateLatestTimestampSql;
        private readonly string updateStartTimestampSql;
        readonly ISerializer serializer;
        public SnapshotStorage(ISerializer serializer, StorageConfig table)
        {
            this.serializer = serializer;
            tableInfo = table;
            deleteSql = $"DELETE FROM {tableInfo.SnapshotTable} where stateid=@StateId";
            getByIdSql = $"select * FROM {tableInfo.SnapshotTable} where stateid=@StateId";
            insertSql = $"INSERT into {tableInfo.SnapshotTable}(stateid,data,version,StartTimestamp,LatestMinEventTimestamp,IsLatest,IsOver)VALUES(@StateId,(@Data)::jsonb,@Version,@StartTimestamp,@LatestMinEventTimestamp,@IsLatest,@IsOver)";
            updateSql = $"update {tableInfo.SnapshotTable} set data=(@Data)::jsonb,version=@Version,LatestMinEventTimestamp=@LatestMinEventTimestamp,IsLatest=@IsLatest,IsOver=@IsOver where stateid=@StateId";
            updateOverSql = $"update {tableInfo.SnapshotTable} set IsOver=@IsOver where stateid=@StateId";
            updateIsLatestSql = $"update {tableInfo.SnapshotTable} set IsLatest=@IsLatest where stateid=@StateId";
            updateLatestTimestampSql = $"update {tableInfo.SnapshotTable} set LatestMinEventTimestamp=@LatestMinEventTimestamp where stateid=@StateId";
            updateStartTimestampSql = $"update {tableInfo.SnapshotTable} set StartTimestamp=@StartTimestamp where stateid=@StateId";
        }
        public async Task Delete(PrimaryKey id)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                await conn.ExecuteAsync(deleteSql, new
                {
                    StateId = id.ToString()
                });
            }
        }
        public async Task Insert(Snapshot<PrimaryKey, StateType> snapshot)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(insertSql, new
                {
                    StateId = snapshot.Base.StateId.ToString(),
                    Data = serializer.SerializeToString(snapshot.State),
                    snapshot.Base.Version,
                    snapshot.Base.StartTimestamp,
                    snapshot.Base.LatestMinEventTimestamp,
                    snapshot.Base.IsLatest,
                    snapshot.Base.IsOver
                });
            }
        }
        public async Task Update(Snapshot<PrimaryKey, StateType> snapshot)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateSql, new
                {
                    StateId = snapshot.Base.StateId.ToString(),
                    Data = serializer.SerializeToString(snapshot.State),
                    snapshot.Base.Version,
                    snapshot.Base.LatestMinEventTimestamp,
                    snapshot.Base.IsLatest,
                    snapshot.Base.IsOver
                });
            }
        }
        public async Task Over(PrimaryKey id, bool isOver)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateOverSql, new { StateId = id, IsOver = isOver });
            }
        }
        public async Task UpdateIsLatest(PrimaryKey id, bool isLatest)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateIsLatestSql, new
                {
                    StateId = id.ToString(),
                    IsLatest = isLatest
                });
            }
        }

        public async Task UpdateLatestMinEventTimestamp(PrimaryKey id, long timestamp)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateLatestTimestampSql, new
                {
                    StateId = id.ToString(),
                    LatestMinEventTimestamp = timestamp
                });
            }
        }
        public async Task UpdateStartTimestamp(PrimaryKey id, long timestamp)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateStartTimestampSql, new
                {
                    StateId = id.ToString(),
                    StartTimestamp = timestamp
                });
            }
        }

        public async Task<Snapshot<PrimaryKey, StateType>> Get(PrimaryKey id)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                var data = await conn.QuerySingleOrDefaultAsync<StateModel>(getByIdSql, new { StateId = id.ToString() });
                if (data != default)
                {
                    return new Snapshot<PrimaryKey, StateType>()
                    {
                        Base = new SnapshotBase<PrimaryKey>
                        {
                            StateId = id,
                            Version = data.Version,
                            DoingVersion = data.Version,
                            IsLatest = data.IsLatest,
                            IsOver = data.IsOver,
                            StartTimestamp = data.StartTimestamp,
                            LatestMinEventTimestamp = data.LatestMinEventTimestamp
                        },
                        State = serializer.Deserialize<StateType>(data.Data)
                    };
                }
            }
            return default;
        }
    }
}
