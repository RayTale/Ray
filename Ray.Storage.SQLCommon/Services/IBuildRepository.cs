using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Storage.SQLCore.Services
{
    public interface IBuildRepository
    {
        Task<List<EventSubTable>> GetSubTableList();
        Task<bool> CreateEventSubRecordTable();
        Task CreateEventTable(EventSubTable subTable);
        Task CreateSnapshotTable();
        Task CreateFollowSnapshotTable(string followSnapshotTable);
        Task CreateSnapshotArchiveTable();
        Task CreateEventArchiveTable();
    }
}
