using System.IO;
using System.Threading.Tasks;
using Dapper;
using Ray.Core.Serialization;
using Ray.Core.State;
using Ray.Core.Storage;
using Ray.Core.Utils;

namespace Ray.Storage.PostgreSQL
{
    public class SqlStateStorage<K, S, B> : ISnapshotStorage<K, S, B>
        where S : class, IState<K, B>
        where B : ISnapshot<K>, new()
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

        public async Task<S> Get(K id)
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
                    return serializer.Deserialize<S>(ms);
                }
            }
            return null;
        }

        public async Task Insert(S data)
        {
            using (var ms = new PooledMemoryStream())
            {
                serializer.Serialize(ms, data);
                using (var connection = tableInfo.CreateConnection())
                {
                    await connection.ExecuteAsync(insertSql, new { StateId = data.Base.StateId.ToString(), Data = ms.ToArray(), data.Base.Version });
                }
            }
        }

        public Task Over(K id)
        {
            //TODO
            throw new System.NotImplementedException();
        }

        public async Task Update(S data)
        {
            using (var ms = new PooledMemoryStream())
            {
                serializer.Serialize(ms, data);
                using (var connection = tableInfo.CreateConnection())
                {
                    await connection.ExecuteAsync(updateSql, new { StateId = data.Base.StateId.ToString(), Data = ms.ToArray(), data.Base.Version });
                }
            }
        }
    }
}
