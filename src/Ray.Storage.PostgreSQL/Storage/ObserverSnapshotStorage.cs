using System.Threading.Tasks;
using Dapper;
using Ray.Core.Snapshot;
using Ray.Core.Storage;
using Ray.Storage.SQLCore;
using Ray.Storage.SQLCore.Configuration;

namespace Ray.Storage.PostgreSQL
{
    public class ObserverSnapshotStorage<PrimaryKey> : IObserverSnapshotStorage<PrimaryKey>
    {
        readonly ObserverStorageOptions config;
        private readonly string deleteSql;
        private readonly string getByIdSql;
        private readonly string insertSql;
        private readonly string updateSql;
        private readonly string updateStartTimestampSql;
        public ObserverSnapshotStorage(ObserverStorageOptions config)
        {
            this.config = config;
            var observerStateTable = config.ObserverSnapshotTable;
            deleteSql = $"DELETE FROM {observerStateTable} where stateid=@StateId";
            getByIdSql = $"select * FROM {observerStateTable} where stateid=@StateId";
            insertSql = $"INSERT into {observerStateTable}(stateid,version,StartTimestamp)VALUES(@StateId,@Version,@StartTimestamp)";
            updateSql = $"update {observerStateTable} set version=@Version,StartTimestamp=@StartTimestamp where stateid=@StateId";
            updateStartTimestampSql = $"update {observerStateTable} set StartTimestamp=@StartTimestamp where stateid=@StateId";
        }
        public async Task<ObserverSnapshot<PrimaryKey>> Get(PrimaryKey id)
        {
            using var conn = config.CreateConnection();
            var data = await conn.QuerySingleOrDefaultAsync<ObserverSnapshotModel>(getByIdSql, new { StateId = id });
            if (data != null)
            {
                return new ObserverSnapshot<PrimaryKey>()
                {
                    StateId = id,
                    Version = data.Version,
                    DoingVersion = data.Version,
                    StartTimestamp = data.StartTimestamp
                };
            }
            return default;
        }
        public async Task Insert(ObserverSnapshot<PrimaryKey> snapshot)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(insertSql, new
            {
                snapshot.StateId,
                snapshot.Version,
                snapshot.StartTimestamp
            });
        }

        public async Task Update(ObserverSnapshot<PrimaryKey> snapshot)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(updateSql, new
            {
                snapshot.StateId,
                snapshot.Version,
                snapshot.StartTimestamp
            });
        }
        public async Task Delete(PrimaryKey id)
        {
            using var conn = config.CreateConnection();
            await conn.ExecuteAsync(deleteSql, new { StateId = id });
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
    }
}
