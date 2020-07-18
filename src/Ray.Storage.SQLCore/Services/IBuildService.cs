using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Storage.SQLCore.Services
{
    public interface IBuildService
    {
        Task<List<EventSubTable>> GetSubTables();
        Task<bool> CreateEventSubTable();
        Task CreateEventTable(EventSubTable subTable);
        Task CreateSnapshotTable();
        Task CreateObserverSnapshotTable(string followSnapshotTable);
        Task CreateSnapshotArchiveTable();
        Task CreateEventArchiveTable();
    }
}
