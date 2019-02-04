using System.Threading.Tasks;
using Dapper;
using Ray.Core.State;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class FollowSnapshotStorage<K> : IFollowSnapshotStorage<K>
    {
        readonly StorageConfig tableInfo;
        private readonly string deleteSql;
        private readonly string getByIdSql;
        private readonly string insertSql;
        private readonly string updateSql;
        public FollowSnapshotStorage(StorageConfig table)
        {
            tableInfo = table;
            var followStateTable = table.GetFollowStateTable();
            deleteSql = $"DELETE FROM {followStateTable} where stateid=@StateId";
            getByIdSql = $"select * FROM {followStateTable} where stateid=@StateId";
            insertSql = $"INSERT into {followStateTable}(stateid,version,StartTimestamp)VALUES(@StateId,@Version,@StartTimestamp)";
            updateSql = $"update {followStateTable} set version=@Version,StartTimestamp=@StartTimestamp where stateid=@StateId";
        }
        public async Task<FollowSnapshot<K>> Get(K id)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                var data = await conn.QuerySingleOrDefaultAsync<FollowStateModel>(getByIdSql, new { StateId = id.ToString() });
                if (data != default)
                {
                    K stateId = default;
                    if (typeof(K) == typeof(long) && long.Parse(data.StateId) is K longValue)
                        stateId = longValue;
                    else if (data.StateId is K stringValue)
                        stateId = stringValue;
                    return new FollowSnapshot<K>()
                    {
                        StateId = stateId,
                        Version = data.Version,
                        DoingVersion = data.Version,
                        StartTimestamp = data.StartTimestamp
                    };
                }
            }
            return default;
        }
        public async Task Insert(FollowSnapshot<K> data)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(insertSql, new
                {
                    StateId = data.StateId.ToString(),
                    data.Version,
                    data.StartTimestamp
                });
            }
        }

        public async Task Update(FollowSnapshot<K> data)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateSql, new
                {
                    StateId = data.StateId.ToString(),
                    data.Version,
                    data.StartTimestamp
                });
            }
        }
        public async Task Delete(K id)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                await conn.ExecuteAsync(deleteSql, new { StateId = id.ToString() });
            }
        }
    }
}
