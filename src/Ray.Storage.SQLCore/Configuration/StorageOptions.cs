using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Serialization;
using Ray.Core.Storage;
using Ray.Storage.SQLCore.Services;

namespace Ray.Storage.SQLCore.Configuration
{
    public abstract class StorageOptions : IStorageOptions
    {
        readonly ILogger logger;
        private readonly ISerializer serializer;
        public StorageOptions(IServiceProvider serviceProvider)
        {
            logger = serviceProvider.GetService<ILogger<StorageOptions>>();
            serializer = serviceProvider.GetService<ISerializer>();
        }
        public bool Singleton { get; set; }
        /// <summary>
        /// 并非唯一，Grain可以同名（不同命名空间下）,且无要求唯一的必要性
        /// <remarks>代码更改可以兼容当前已经使用UniqueName的地方，应在不兼容版本升级时，删除这个属性</remarks>
        /// </summary>
        [Obsolete("请使用GrainStorageName替代")]
        public string UniqueName
        {
            get => this.GrainStorageName;
            set => this.GrainStorageName = value;
        }

        /// <summary>
        /// Grain对应的存储名称
        /// <remarks>
        /// for example:
        ///  sql      --> table name
        ///  mongodb  --> collection name
        /// </remarks>
        /// </summary>

        public string GrainStorageName { get; set; }
        /// <summary>
        /// 分表间隔时间
        /// 设置为0时表示不分表
        /// </summary>
        public long SubTableMillionSecondsInterval { get; set; }
        public string EventTable => $"{GrainStorageName}_Event";
        public string SnapshotTable => $"{GrainStorageName}_Snapshot";
        public string SnapshotArchiveTable => $"{SnapshotTable}_Archive";
        public string EventArchiveTable => $"{EventTable}_Archive";
        public string Connection { get; set; }
        public Func<string, DbConnection> CreateConnectionFunc { get; set; }
        public DbConnection CreateConnection() => CreateConnectionFunc(Connection);
        public IBuildService BuildRepository { get; set; }
        private List<EventSubTable> _subTables;
        readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);
        public async ValueTask Build()
        {
            if (!await BuildRepository.CreateEventSubTable())
            {
                _subTables = (await BuildRepository.GetSubTables()).OrderBy(table => table.EndTime).ToList();
            }
            await BuildRepository.CreateSnapshotTable();
            await BuildRepository.CreateSnapshotArchiveTable();
            await BuildRepository.CreateEventArchiveTable();
        }

        public async ValueTask<List<EventSubTable>> GetSubTables()
        {
            var lastSubTable = _subTables.LastOrDefault();
            if (lastSubTable is null || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
            {
                await semaphore.WaitAsync();
                try
                {
                    if (lastSubTable is null || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                    {
                        _subTables = (await BuildRepository.GetSubTables()).OrderBy(table => table.EndTime).ToList();
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }
            return _subTables;
        }
        public async ValueTask<EventSubTable> GetTable(long eventTimestamp)
        {
            var getTask = GetSubTables();
            if (!getTask.IsCompletedSuccessfully)
                await getTask;
            var subTable = SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
            if (subTable is null)
            {
                await semaphore.WaitAsync();
                subTable = SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
                try
                {
                    if (subTable is null)
                    {
                        var lastSubTable = getTask.Result.LastOrDefault();
                        var startTime = lastSubTable != null ? (lastSubTable.EndTime == lastSubTable.StartTime ? eventTimestamp : lastSubTable.EndTime) : eventTimestamp;
                        if (eventTimestamp >= startTime)
                        {
                            var index = lastSubTable is null ? 0 : lastSubTable.Index + 1;
                            subTable = new EventSubTable
                            {
                                TableName = EventTable,
                                SubTable = $"{EventTable}_{index}",
                                Index = index,
                                StartTime = startTime,
                                EndTime = startTime + SubTableMillionSecondsInterval
                            };
                        }
                        else
                        {
                            var firstSubTable = getTask.Result.FirstOrDefault();
                            var index = firstSubTable.Index - 1;
                            subTable = new EventSubTable
                            {
                                TableName = EventTable,
                                SubTable = index < 0 ? $"{EventTable}_n_{index * -1}" : $"{EventTable}_{index}",
                                Index = index,
                                StartTime = firstSubTable.StartTime - SubTableMillionSecondsInterval,
                                EndTime = firstSubTable.StartTime
                            };
                        }
                        try
                        {
                            await BuildRepository.CreateEventTable(subTable);
                            _subTables.Add(subTable);
                            subTable = _subTables.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
                        }
                        catch (Exception ex)
                        {
                            logger.LogCritical(ex, serializer.Serialize(subTable));
                            subTable = default;
                            _subTables = (await BuildRepository.GetSubTables()).OrderBy(table => table.EndTime).ToList();
                        }
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }
            if (subTable is null)
            {
                subTable = await GetTable(eventTimestamp);
            }
            return subTable;
        }
    }
}
