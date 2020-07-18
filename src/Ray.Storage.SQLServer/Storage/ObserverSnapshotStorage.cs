using System.Threading.Tasks;
using Dapper;
using Ray.Core.Snapshot;
using Ray.Core.Storage;
using Ray.Storage.SQLCore;
using Ray.Storage.SQLCore.Configuration;

namespace Ray.Storage.SQLServer
{
    public class ObserverSnapshotStorage<PrimaryKey> : IObserverSnapshotStorage<PrimaryKey>
    {
        private readonly ObserverStorageOptions config;
        private readonly string deleteSql;
        private readonly string getByIdSql;
        private readonly string insertSql;
        private readonly string updateSql;
        private readonly string updateStartTimestampSql;

        public ObserverSnapshotStorage(ObserverStorageOptions config)
        {
            this.config = config;
            var observerStateTable = config.ObserverSnapshotTable;
            this.deleteSql = $"DELETE FROM {observerStateTable} where stateid=@StateId";
            this.getByIdSql = $"select * FROM {observerStateTable} where stateid=@StateId";
            this.insertSql = $"INSERT into {observerStateTable}(stateid,version,StartTimestamp)VALUES(@StateId,@Version,@StartTimestamp)";
            this.updateSql = $"update {observerStateTable} set version=@Version,StartTimestamp=@StartTimestamp where stateid=@StateId";
            this.updateStartTimestampSql = $"update {observerStateTable} set StartTimestamp=@StartTimestamp where stateid=@StateId";
        }

        public async Task<ObserverSnapshot<PrimaryKey>> Get(PrimaryKey id)
        {
            using var conn = this.config.CreateConnection();
            var data = await conn.QuerySingleOrDefaultAsync<ObserverSnapshotModel>(this.getByIdSql, new { StateId = id });
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
            using var connection = this.config.CreateConnection();
            await connection.ExecuteAsync(this.insertSql, new
            {
                snapshot.StateId,
                snapshot.Version,
                snapshot.StartTimestamp
            });
        }

        public async Task Update(ObserverSnapshot<PrimaryKey> snapshot)
        {
            using var connection = this.config.CreateConnection();
            await connection.ExecuteAsync(this.updateSql, new
            {
                snapshot.StateId,
                snapshot.Version,
                snapshot.StartTimestamp
            });
        }

        public async Task Delete(PrimaryKey id)
        {
            using var conn = this.config.CreateConnection();
            await conn.ExecuteAsync(this.deleteSql, new { StateId = id });
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
    }
}
