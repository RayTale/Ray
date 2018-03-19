using Ray.Core.EventSourcing;
using System.Threading.Tasks;
using Dapper;
using System.IO;
using ProtoBuf;
using Ray.Core.Utils;

namespace Ray.Postgresql
{
    public class StateStorage<T, K> : IStateStorage<T, K> where T : class, IState<K>
    {
        SqlTable tableInfo;
        string deleteSql, getByIdSql, insertSql, updateSql;
        public StateStorage(SqlTable table)
        {
            this.tableInfo = table;
            deleteSql = $"DELETE FROM {tableInfo.SnapshotTable} where stateid=@StateId";
            getByIdSql = $"select data FROM {tableInfo.SnapshotTable} where stateid=@StateId";
            insertSql = $"INSERT into {tableInfo.SnapshotTable}(stateid,data)VALUES(@StateId,@Data)";
            updateSql = $"update {tableInfo.SnapshotTable} set data=Data where stateid=@StateId";
        }
        public Task DeleteAsync(K id)
        {
            return RayTask.Execute(async () =>
            {
                using (var conn = tableInfo.CreateConnection())
                {
                    await conn.ExecuteAsync(deleteSql, new { StateId = id });
                }
            });
        }

        public async Task<T> GetByIdAsync(K id)
        {
            byte[] state = await RayTask.Execute(async () =>
            {
                using (var conn = tableInfo.CreateConnection())
                {
                    return await conn.ExecuteScalarAsync<byte[]>(getByIdSql, new { StateId = id });
                }
            });
            if (state != null)
            {
                using (var ms = new MemoryStream(state))
                {
                    return Serializer.Deserialize<T>(ms);
                }
            }
            return null;
        }

        public Task InsertAsync(T data)
        {
            return RayTask.Execute(async () =>
            {
                using (var ms = new PooledMemoryStream())
                {
                    Serializer.Serialize(ms, data);
                    using (var connection = tableInfo.CreateConnection())
                    {
                        await connection.ExecuteAsync(insertSql, new { data.StateId, Data = ms.ToArray() });
                    }
                }
            });
        }

        public Task UpdateAsync(T data)
        {
            return RayTask.Execute(async () =>
            {
                using (var ms = new PooledMemoryStream())
                {
                    Serializer.Serialize(ms, data);
                    using (var connection = tableInfo.CreateConnection())
                    {
                        await connection.ExecuteAsync(updateSql, new { data.StateId, Data = ms.ToArray() });
                    }
                }
            });
        }
    }
}
