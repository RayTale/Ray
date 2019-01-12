using System.IO;
using System.Threading.Tasks;
using Dapper;
using Ray.Core.Serialization;
using Ray.Core.State;
using Ray.Core.Storage;
using Ray.Core.Utils;

namespace Ray.Storage.PostgreSQL
{
    public class SqlStateStorage<K, T> : IStateStorage<K, T>
        where T : class, IActorState<K>
    {
        readonly StorageConfig tableInfo;
        private readonly string deleteSql;
        private readonly string getByIdSql;
        private readonly string insertSql;
        private readonly string updateSql;
        readonly ISerializer serializer;
        public SqlStateStorage(ISerializer serializer, StorageConfig table)
        {
            this.serializer = serializer;
            tableInfo = table;
            deleteSql = $"DELETE FROM {tableInfo.SnapshotTable} where stateid=@StateId";
            getByIdSql = $"select data FROM {tableInfo.SnapshotTable} where stateid=@StateId";
            insertSql = $"INSERT into {tableInfo.SnapshotTable}(stateid,data,version)VALUES(@StateId,@Data,@Version)";
            updateSql = $"update {tableInfo.SnapshotTable} set data=@Data,version=@Version where stateid=@StateId";
        }
        public async Task Delete(K id)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                await conn.ExecuteAsync(deleteSql, new { StateId = id.ToString() });
            }
        }

        public async Task<T> Get(K id)
        {
            byte[] state;
            using (var conn = tableInfo.CreateConnection())
            {
                state = await conn.ExecuteScalarAsync<byte[]>(getByIdSql, new { StateId = id.ToString() });
            }
            if (state != null)
            {
                using (var ms = new MemoryStream(state))
                {
                    return serializer.Deserialize<T>(ms);
                }
            }
            return null;
        }

        public async Task Insert(T data)
        {
            using (var ms = new PooledMemoryStream())
            {
                serializer.Serialize(ms, data);
                using (var connection = tableInfo.CreateConnection())
                {
                    await connection.ExecuteAsync(insertSql, new { StateId = data.StateId.ToString(), Data = ms.ToArray(), data.Version });
                }
            }
        }

        public async Task Update(T data)
        {
            using (var ms = new PooledMemoryStream())
            {
                serializer.Serialize(ms, data);
                using (var connection = tableInfo.CreateConnection())
                {
                    await connection.ExecuteAsync(updateSql, new { StateId = data.StateId.ToString(), Data = ms.ToArray(), data.Version });
                }
            }
        }
    }
}
