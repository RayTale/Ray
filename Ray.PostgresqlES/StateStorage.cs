using Ray.Core.EventSourcing;
using System.Threading.Tasks;
using Dapper;
using System.IO;
using Ray.PostgresqlES.Utils;
using ProtoBuf;

namespace Ray.PostgresqlES
{
    public class StateStorage<T, K> : IStateStorage<T, K> where T : class, IState<K>
    {
        SqlTable tableInfo;
        string deleteSql, getByIdSql, insertSql, updateSql;
        public StateStorage(SqlTable mongoAttr)
        {
            this.tableInfo = mongoAttr;
            deleteSql = $"DELETE FROM {tableInfo.SnapshotTable} where stateid=@StateId";
            getByIdSql = $"select data FROM {tableInfo.SnapshotTable} where stateid=@StateId";
            insertSql = $"INSERT {tableInfo.SnapshotTable}(stateid,data)VALUES(@StateId,@Data)";
            updateSql = $"update {tableInfo.SnapshotTable} set data=@Data where stateid=@StateId";
        }
        public async Task DeleteAsync(K id)
        {
            using (var conn = tableInfo.CreateConnection())
            {
                await conn.ExecuteAsync(deleteSql, new { StateId = id });
            }
        }

        public async Task<T> GetByIdAsync(K id)
        {
            string state;
            using (var conn = tableInfo.CreateConnection())
            {
                state = await conn.ExecuteScalarAsync<string>(getByIdSql, new { StateId = id });
            }
            if (state != null)
            {
                using (MemoryStream ms = new MemoryStream(Hex.HexToBytes(state)))
                {
                    return Serializer.Deserialize<T>(ms);
                }
            }
            return null;
        }

        public async Task InsertAsync(T data)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                Serializer.Serialize<T>(ms, data);
                using (var connection = tableInfo.CreateConnection())
                {
                    await connection.ExecuteAsync(insertSql, new { data.StateId, Data = Hex.BytesToHex(ms.ToArray()) });
                }
            }
        }

        public async Task UpdateAsync(T data)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                Serializer.Serialize<T>(ms, data);
                using (var connection = tableInfo.CreateConnection())
                {
                    await connection.ExecuteAsync(updateSql, new { data.StateId, Data = Hex.BytesToHex(ms.ToArray()) });
                }
            }
        }
    }
}
