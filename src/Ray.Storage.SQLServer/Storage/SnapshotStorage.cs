using System.Threading.Tasks;
using Dapper;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;
using Ray.Storage.SQLCore;
using Ray.Storage.SQLCore.Configuration;

namespace Ray.Storage.SQLServer
{
    public class SnapshotStorage<PrimaryKey, StateType> : ISnapshotStorage<PrimaryKey, StateType>
        where StateType : class, new()
    {
        private readonly StorageOptions config;
        private readonly string deleteSql;
        private readonly string getByIdSql;
        private readonly string insertSql;
        private readonly string updateSql;
        private readonly string updateOverSql;
        private readonly string updateIsLatestSql;
        private readonly string updateLatestTimestampSql;
        private readonly string updateStartTimestampSql;
        private readonly ISerializer serializer;

        public SnapshotStorage(ISerializer serializer, StorageOptions config)
        {
            this.serializer = serializer;
            this.config = config;
            this.deleteSql = $"DELETE FROM {this.config.SnapshotTable} where stateid=@StateId";
            this.getByIdSql = $"select * FROM {this.config.SnapshotTable} where stateid=@StateId";
            this.insertSql = $"INSERT into {this.config.SnapshotTable}(stateid,data,version,StartTimestamp,LatestMinEventTimestamp,IsLatest,IsOver)VALUES(@StateId,@Data,@Version,@StartTimestamp,@LatestMinEventTimestamp,@IsLatest,@IsOver)";
            this.updateSql = $"update {this.config.SnapshotTable} set data=@Data,version=@Version,LatestMinEventTimestamp=@LatestMinEventTimestamp,IsLatest=@IsLatest,IsOver=@IsOver where stateid=@StateId";
            this.updateOverSql = $"update {this.config.SnapshotTable} set IsOver=@IsOver where stateid=@StateId";
            this.updateIsLatestSql = $"update {this.config.SnapshotTable} set IsLatest=@IsLatest where stateid=@StateId";
            this.updateLatestTimestampSql = $"update {this.config.SnapshotTable} set LatestMinEventTimestamp=@LatestMinEventTimestamp where stateid=@StateId";
            this.updateStartTimestampSql = $"update {this.config.SnapshotTable} set StartTimestamp=@StartTimestamp where stateid=@StateId";
        }

        public async Task Delete(PrimaryKey id)
        {
            using var conn = this.config.CreateConnection();
            await conn.ExecuteAsync(this.deleteSql, new
            {
                StateId = id
            });
        }

        public async Task Insert(Snapshot<PrimaryKey, StateType> snapshot)
        {
            using var connection = this.config.CreateConnection();
            await connection.ExecuteAsync(this.insertSql, new
            {
                snapshot.Base.StateId,
                Data = this.serializer.Serialize(snapshot.State),
                snapshot.Base.Version,
                snapshot.Base.StartTimestamp,
                snapshot.Base.LatestMinEventTimestamp,
                snapshot.Base.IsLatest,
                snapshot.Base.IsOver
            });
        }

        public async Task Update(Snapshot<PrimaryKey, StateType> snapshot)
        {
            using var connection = this.config.CreateConnection();
            await connection.ExecuteAsync(this.updateSql, new
            {
                snapshot.Base.StateId,
                Data = this.serializer.Serialize(snapshot.State),
                snapshot.Base.Version,
                snapshot.Base.LatestMinEventTimestamp,
                snapshot.Base.IsLatest,
                snapshot.Base.IsOver
            });
        }

        public async Task Over(PrimaryKey id, bool isOver)
        {
            using var connection = this.config.CreateConnection();
            await connection.ExecuteAsync(this.updateOverSql, new { StateId = id, IsOver = isOver });
        }

        public async Task UpdateIsLatest(PrimaryKey id, bool isLatest)
        {
            using var connection = this.config.CreateConnection();
            await connection.ExecuteAsync(this.updateIsLatestSql, new
            {
                StateId = id,
                IsLatest = isLatest
            });
        }

        public async Task UpdateLatestMinEventTimestamp(PrimaryKey id, long timestamp)
        {
            using var connection = this.config.CreateConnection();
            await connection.ExecuteAsync(this.updateLatestTimestampSql, new
            {
                StateId = id,
                LatestMinEventTimestamp = timestamp
            });
        }

        public async Task UpdateStartTimestamp(PrimaryKey id, long timestamp)
        {
            using var connection = this.config.CreateConnection();
            await connection.ExecuteAsync(this.updateStartTimestampSql, new
            {
                StateId = id,
                StartTimestamp = timestamp
            });
        }

        public async Task<Snapshot<PrimaryKey, StateType>> Get(PrimaryKey id)
        {
            using var conn = this.config.CreateConnection();
            var data = await conn.QuerySingleOrDefaultAsync<SnapshotModel<PrimaryKey>>(this.getByIdSql, new { StateId = id });
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
                    State = this.serializer.Deserialize<StateType>(data.Data)
                };
            }

            return default;
        }
    }
}
