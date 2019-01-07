using System.IO;
using System.Threading.Tasks;
using Dapper;
using ProtoBuf;
using Ray.Core.State;
using Ray.Core.Storage;
using Ray.Core.Utils;

namespace Ray.Storage.PostgreSQL
{
    public class SqlStateStorage<T, K> : IStateStorage<T, K> where T : class, IState<K>
    {
        readonly SqlGrainConfig tableInfo;
        private readonly string deleteSql;
        private readonly string getByIdSql;
        private readonly string insertSql;
        private readonly string updateSql;

        public SqlStateStorage(SqlGrainConfig table)
        {
            tableInfo = table;
            deleteSql = $"DELETE FROM {tableInfo.SnapshotTable} where stateid=@StateId";
            getByIdSql = $"select data FROM {tableInfo.SnapshotTable} where stateid=@StateId";
            insertSql = $"INSERT into {tableInfo.SnapshotTable}(stateid,data,version)VALUES(@StateId,@Data,@Version)";
            updateSql = $"update {tableInfo.SnapshotTable} set data=@Data,version=@Version where stateid=@StateId";
        }
        public async Task DeleteAsync(K id)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                await conn.ExecuteAsync(deleteSql, new { StateId = id.ToString() });
            }
        }

        public async Task<T> GetByIdAsync(K id)
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
                    return Serializer.Deserialize<T>(ms);
                }
            }
            return null;
        }

        public async Task InsertAsync(T data)
        {
            using (var ms = new PooledMemoryStream())
            {
                Serializer.Serialize(ms, data);
                using (var connection = tableInfo.CreateConnection())
                {
                    await connection.ExecuteAsync(insertSql, new { StateId = data.StateId.ToString(), Data = ms.ToArray(), data.Version });
                }
            }
        }

        public async Task UpdateAsync(T data)
        {
            using (var ms = new PooledMemoryStream())
            {
                Serializer.Serialize(ms, data);
                using (var connection = tableInfo.CreateConnection())
                {
                    await connection.ExecuteAsync(updateSql, new { StateId = data.StateId.ToString(), Data = ms.ToArray(), data.Version });
                }
            }
        }
    }
}
