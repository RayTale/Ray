using System.Threading.Tasks;
using Dapper;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;
using Ray.Storage.SQLCore;
using Ray.Storage.SQLCore.Configuration;

namespace Ray.Storage.PostgreSQL
{
    public class SnapshotStorage<PrimaryKey, StateType> : ISnapshotStorage<PrimaryKey, StateType>
        where StateType : class, new()
    {
        readonly StorageOptions config;
        private readonly string deleteSql;
        private readonly string getByIdSql;
        private readonly string insertSql;
        private readonly string updateSql;
        private readonly string updateOverSql;
        private readonly string updateIsLatestSql;
        private readonly string updateLatestTimestampSql;
        private readonly string updateStartTimestampSql;
        readonly ISerializer serializer;
        public SnapshotStorage(ISerializer serializer, StorageOptions config)
        {
            this.serializer = serializer;
            this.config = config;
            deleteSql = $"DELETE FROM {this.config.SnapshotTable} where stateid=@StateId";
            getByIdSql = $"select * FROM {this.config.SnapshotTable} where stateid=@StateId";
            insertSql = $"INSERT into {this.config.SnapshotTable}(stateid,data,version,StartTimestamp,LatestMinEventTimestamp,IsLatest,IsOver)VALUES(@StateId,(@Data)::json,@Version,@StartTimestamp,@LatestMinEventTimestamp,@IsLatest,@IsOver)";
            updateSql = $"update {this.config.SnapshotTable} set data=(@Data)::json,version=@Version,LatestMinEventTimestamp=@LatestMinEventTimestamp,IsLatest=@IsLatest,IsOver=@IsOver where stateid=@StateId";
            updateOverSql = $"update {this.config.SnapshotTable} set IsOver=@IsOver where stateid=@StateId";
            updateIsLatestSql = $"update {this.config.SnapshotTable} set IsLatest=@IsLatest where stateid=@StateId";
            updateLatestTimestampSql = $"update {this.config.SnapshotTable} set LatestMinEventTimestamp=@LatestMinEventTimestamp where stateid=@StateId";
            updateStartTimestampSql = $"update {this.config.SnapshotTable} set StartTimestamp=@StartTimestamp where stateid=@StateId";
        }
        public async Task Delete(PrimaryKey id)
        {
            using var conn = config.CreateConnection();
            await conn.ExecuteAsync(deleteSql, new
            {
                StateId = id
            });
        }
        public async Task Insert(Snapshot<PrimaryKey, StateType> snapshot)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(insertSql, new
            {
                snapshot.Base.StateId,
                Data = serializer.Serialize(snapshot.State),
                snapshot.Base.Version,
                snapshot.Base.StartTimestamp,
                snapshot.Base.LatestMinEventTimestamp,
                snapshot.Base.IsLatest,
                snapshot.Base.IsOver
            });
        }
        public async Task Update(Snapshot<PrimaryKey, StateType> snapshot)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(updateSql, new
            {
                snapshot.Base.StateId,
                Data = serializer.Serialize(snapshot.State),
                snapshot.Base.Version,
                snapshot.Base.LatestMinEventTimestamp,
                snapshot.Base.IsLatest,
                snapshot.Base.IsOver
            });
        }
        public async Task Over(PrimaryKey id, bool isOver)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(updateOverSql, new { StateId = id, IsOver = isOver });
        }
        public async Task UpdateIsLatest(PrimaryKey id, bool isLatest)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(updateIsLatestSql, new
            {
                StateId = id,
                IsLatest = isLatest
            });
        }

        public async Task UpdateLatestMinEventTimestamp(PrimaryKey id, long timestamp)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(updateLatestTimestampSql, new
            {
                StateId = id,
                LatestMinEventTimestamp = timestamp
            });
        }
        public async Task UpdateStartTimestamp(PrimaryKey id, long timestamp)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(updateStartTimestampSql, new
            {
                StateId = id,
                StartTimestamp = timestamp
            });
        }

        public async Task<Snapshot<PrimaryKey, StateType>> Get(PrimaryKey id)
        {
            using var conn = config.CreateConnection();
            var data = await conn.QuerySingleOrDefaultAsync<SnapshotModel<PrimaryKey>>(getByIdSql, new { StateId = id });
            if (data != null)
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
            return default;
        }
    }
}
