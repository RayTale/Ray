using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ray.Core.Serialization;
using Ray.Core.Storage;
using Ray.Storage.PostgreSQL.Entitys;
using Ray.Storage.PostgreSQL.Services.Abstractions;

namespace Ray.Storage.PostgreSQL
{
    public class StorageConfig : IStorageConfig
    {
        private readonly ITableRepository tableRepository;
        private readonly ILogger<StorageConfig> logger;
        private readonly ISerializer serializer;
        public StorageConfig(IServiceProvider serviceProvider, string connectionKey, string eventTable, string snapshotTable, long subTableMinutesInterval = 40, int stateIdLength = 200)
        {
            Connection = serviceProvider.GetService<IOptions<SqlConfig>>().Value.ConnectionDict[connectionKey];
            tableRepository = serviceProvider.GetService<ITableRepository>();
            logger = serviceProvider.GetService<ILogger<StorageConfig>>();
            serializer = serviceProvider.GetService<ISerializer>();
            EventTable = eventTable;
            SnapshotTable = snapshotTable;
            SubTableMillionSecondsInterval = subTableMinutesInterval * 24 * 60 * 60 * 1000;
            StateIdLength = stateIdLength;
        }
        public bool Singleton { get; set; }
        public string Connection { get; set; }
        public string EventTable { get; set; }
        public string SnapshotTable { get; set; }
        public long SubTableMillionSecondsInterval { get; set; }
        public int StateIdLength { get; }
        public string SnapshotArchiveTable => $"{SnapshotTable}_Archive";
        public string EventArchiveTable => $"{EventTable}_Archive";

        bool builded = false;
        private List<SubTableInfo> _subTables;
        readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);
        public async ValueTask Init()
        {
            if (!builded)
            {
                await semaphore.WaitAsync();
                try
                {
                    if (!builded)
                    {
                        if (!await tableRepository.CreateSubRecordTable(Connection))
                        {
                            _subTables = (await tableRepository.GetSubTableList(Connection, EventTable)).OrderBy(table => table.EndTime).ToList();
                        }
                        await tableRepository.CreateSnapshotTable(Connection, SnapshotTable, StateIdLength);
                        await tableRepository.CreateSnapshotArchiveTable(Connection, SnapshotArchiveTable, StateIdLength);
                        await tableRepository.CreateEventArchiveTable(Connection, EventArchiveTable, StateIdLength);
                        builded = true;
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DbConnection CreateConnection()
        {
            return SqlFactory.CreateConnection(Connection);
        }
        public async ValueTask<List<SubTableInfo>> GetSubTables()
        {
            var lastSubTable = _subTables.LastOrDefault();
            if (lastSubTable == default || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
            {
                await semaphore.WaitAsync();
                try
                {
                    if (lastSubTable == default || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                    {
                        _subTables = (await tableRepository.GetSubTableList(Connection, EventTable)).OrderBy(table => table.EndTime).ToList();
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }
            return _subTables;
        }
        public async ValueTask<SubTableInfo> GetTable(long eventTimestamp)
        {
            var getTask = GetSubTables();
            if (!getTask.IsCompletedSuccessfully)
                await getTask;
            var subTable = SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
            if (subTable == default)
            {
                await semaphore.WaitAsync();
                subTable = SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
                try
                {
                    if (subTable == default)
                    {
                        var lastSubTable = getTask.Result.LastOrDefault();
                        var startTime = lastSubTable != default ? (lastSubTable.EndTime == lastSubTable.StartTime ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : lastSubTable.EndTime) : eventTimestamp;
                        var index = lastSubTable == default ? 0 : lastSubTable.Index + 1;
                        subTable = new SubTableInfo
                        {
                            TableName = EventTable,
                            SubTable = $"{EventTable}_{index}",
                            Index = index,
                            StartTime = startTime,
                            EndTime = startTime + SubTableMillionSecondsInterval
                        };
                        try
                        {
                            await tableRepository.CreateEventTable(Connection, subTable, StateIdLength);
                            _subTables.Add(subTable);
                        }
                        catch (Exception ex)
                        {
                            logger.LogCritical(ex, serializer.SerializeToString(subTable));
                            subTable = default;
                            _subTables = (await tableRepository.GetSubTableList(Connection, EventTable)).OrderBy(table => table.EndTime).ToList();
                        }
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }
            if (subTable == default)
            {
                subTable = await GetTable(eventTimestamp);
            }
            return subTable;
        }
    }
}
