using System.Threading.Tasks;
using Dapper;
using Ray.Core.Snapshot;
using Ray.Core.Storage;
using Ray.Storage.SQLCore;
using Ray.Storage.SQLCore.Configuration;

namespace Ray.Storage.PostgreSQL
{
    public class FollowSnapshotStorage<PrimaryKey> : IFollowSnapshotStorage<PrimaryKey>
    {
        readonly FollowStorageConfig config;
        private readonly string deleteSql;
        private readonly string getByIdSql;
        private readonly string insertSql;
        private readonly string updateSql;
        private readonly string updateStartTimestampSql;
        public FollowSnapshotStorage(FollowStorageConfig config)
        {
            this.config = config;
            var followStateTable = config.FollowSnapshotTable;
            deleteSql = $"DELETE FROM {followStateTable} where stateid=@StateId";
            getByIdSql = $"select * FROM {followStateTable} where stateid=@StateId";
            insertSql = $"INSERT into {followStateTable}(stateid,version,StartTimestamp)VALUES(@StateId,@Version,@StartTimestamp)";
            updateSql = $"update {followStateTable} set version=@Version,StartTimestamp=@StartTimestamp where stateid=@StateId";
            updateStartTimestampSql = $"update {followStateTable} set StartTimestamp=@StartTimestamp where stateid=@StateId";
        }
        public async Task<FollowSnapshot<PrimaryKey>> Get(PrimaryKey id)
        {
            using (var conn = config.CreateConnection())
            {
                var data = await conn.QuerySingleOrDefaultAsync<FollowSnapshotModel>(getByIdSql, new { StateId = id });
                if (data != default)
                {
                    return new FollowSnapshot<PrimaryKey>()
                    {
                        StateId = id,
                        Version = data.Version,
                        DoingVersion = data.Version,
                        StartTimestamp = data.StartTimestamp
                    };
                }
            }
            return default;
        }
        public async Task Insert(FollowSnapshot<PrimaryKey> snapshot)
        {
            using (var connection = config.CreateConnection())
            {
                await connection.ExecuteAsync(insertSql, new
                {
                    StateId = snapshot.StateId,
                    snapshot.Version,
                    snapshot.StartTimestamp
                });
            }
        }

        public async Task Update(FollowSnapshot<PrimaryKey> snapshot)
        {
            using (var connection = config.CreateConnection())
            {
                await connection.ExecuteAsync(updateSql, new
                {
                    StateId = snapshot.StateId,
                    snapshot.Version,
                    snapshot.StartTimestamp
                });
            }
        }
        public async Task Delete(PrimaryKey id)
        {
            using (var conn = config.CreateConnection())
            {
                await conn.ExecuteAsync(deleteSql, new { StateId = id });
            }
        }

        public async Task UpdateStartTimestamp(PrimaryKey id, long timestamp)
        {
            using (var connection = config.CreateConnection())
            {
                await connection.ExecuteAsync(updateStartTimestampSql, new
                {
                    StateId = id,
                    StartTimestamp = timestamp
                });
            }
        }
    }
}
