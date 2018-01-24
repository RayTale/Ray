using Ray.Core.EventSourcing;
using System.Threading.Tasks;
using Dapper;
using System.IO;
using ProtoBuf;

namespace Ray.PostgresqlES
{
    public class StateStorage<T, K> : IStateStorage<T, K> where T : class, IState<K>
    {
        SqlTable tableInfo;
        string deleteSql, getByIdSql, insertSql, updateSql;
        public StateStorage(SqlTable mongoAttr,string table)
        {
            this.tableInfo = mongoAttr;
            deleteSql = $"DELETE FROM {table} where stateid=@StateId";
            getByIdSql = $"select data FROM {table} where stateid=@StateId";
            insertSql = $"INSERT into {table}(stateid,data)VALUES(@StateId,@Data)";
            updateSql = $"update {table} set data=Data where stateid=@StateId";
        }
        public Task DeleteAsync(K id)
        {
            return SQLTask.SQLTaskExecute(async () =>
            {
                using (var conn = tableInfo.CreateConnection())
                {
                    await conn.ExecuteAsync(deleteSql, new { StateId = id });
                }
            });
        }

        public async Task<T> GetByIdAsync(K id)
        {
            byte[] state = await SQLTask.SQLTaskExecute<byte[]>(async () =>
            {
                using (var conn = tableInfo.CreateConnection())
                {
                    return await conn.ExecuteScalarAsync<byte[]>(getByIdSql, new { StateId = id });
                }
            });
            if (state != null)
            {
                using (MemoryStream ms = new MemoryStream(state))
                {
                    return Serializer.Deserialize<T>(ms);
                }
            }
            return null;
        }

        public Task InsertAsync(T data)
        {
            return SQLTask.SQLTaskExecute(async () =>
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    Serializer.Serialize<T>(ms, data);
                    using (var connection = tableInfo.CreateConnection())
                    {
                        await connection.ExecuteAsync(insertSql, new { data.StateId, Data = ms.ToArray() });
                    }
                }
            });
        }

        public Task UpdateAsync(T data)
        {
            return SQLTask.SQLTaskExecute(async () =>
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    Serializer.Serialize<T>(ms, data);
                    using (var connection = tableInfo.CreateConnection())
                    {
                        await connection.ExecuteAsync(updateSql, new { data.StateId, Data = ms.ToArray() });
                    }
                }
            });
        }
    }
}
