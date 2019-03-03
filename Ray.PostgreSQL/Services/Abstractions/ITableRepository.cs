using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Storage.PostgreSQL.Entitys;

namespace Ray.Storage.PostgreSQL.Services.Abstractions
{
    public interface ITableRepository
    {
        Task<bool> CreateSubRecordTable(string conn);
        Task<List<SubTableInfo>> GetSubTableList(string conn, string tableName);
        Task CreateEventTable(string conn, SubTableInfo subTable, int stateIdLength);
        Task CreateSnapshotTable(string conn, string tableName, int stateIdLength);
        Task CreateFollowSnapshotTable(string conn, string tableName, int stateIdLength);
        Task CreateSnapshotArchiveTable(string conn, string tableName, int stateIdLength);
        Task CreateEventArchiveTable(string conn, string tableName, int stateIdLength);
    }
}
