using System.Threading.Tasks;
using Dapper;
using Ray.Core.Serialization;
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
        readonly IJsonSerializer serializer;
        public FollowSnapshotStorage(IJsonSerializer serializer, StorageConfig table)
        {
            this.serializer = serializer;
            tableInfo = table;
            var followStateTable = table.GetFollowStateTable();
            deleteSql = $"DELETE FROM {followStateTable} where stateid=@StateId";
            getByIdSql = $"select * FROM {followStateTable} where stateid=@StateId";
            insertSql = $"INSERT into {followStateTable}(stateid,version,doingversion)VALUES(@StateId,@Version,@DoingVersion)";
            updateSql = $"update {followStateTable} set version=@Version,doingversion=@DoingVersion where stateid=@StateId";
        }
        public async Task<FollowSnapshot<K>> Get(K id)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                var data = await conn.QuerySingleOrDefaultAsync<FollowStateModel>(getByIdSql, new { StateId = id.ToString() });
                if (data != default)
                {
                    return new FollowSnapshot<K>()
                    {
                        StateId = serializer.Deserialize<K>(data.StateId),
                        Version = data.Version,
                        DoingVersion = data.DoingVersion
                    };
                }
            }
            return default;
        }
        public async Task Insert(FollowSnapshot<K> data)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(insertSql, new { StateId = data.StateId.ToString(), data.Version, data.DoingVersion });
            }
        }

        public async Task Update(FollowSnapshot<K> data)
        {
            using (var connection = tableInfo.CreateConnection())
            {
                await connection.ExecuteAsync(updateSql, new { StateId = data.StateId.ToString(), data.Version, data.DoingVersion });
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
