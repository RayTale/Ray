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
        private readonly ILogger logger;
        private readonly ISerializer serializer;

        public StorageOptions(IServiceProvider serviceProvider)
        {
            this.logger = serviceProvider.GetService<ILogger<StorageOptions>>();
            this.serializer = serviceProvider.GetService<ISerializer>();
        }

        public bool Singleton { get; set; }

        public string UniqueName { get; set; }

        /// <summary>
        /// 分表间隔时间
        /// 设置为0时表示不分表
        /// </summary>
        public long SubTableMillionSecondsInterval { get; set; }

        public string EventTable => $"{this.UniqueName}_Event";

        public string SnapshotTable => $"{this.UniqueName}_Snapshot";

        public string SnapshotArchiveTable => $"{this.SnapshotTable}_Archive";

        public string EventArchiveTable => $"{this.EventTable}_Archive";

        public string ConnectionKey { get; set; }

        public string Connection { get; set; }

        public Func<string, DbConnection> CreateConnectionFunc { get; set; }

        public DbConnection CreateConnection() => this.CreateConnectionFunc(this.Connection);

        public IBuildService BuildRepository { get; set; }

        private List<EventSubTable> subTables;
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

        public async ValueTask Build()
        {
            if (!await this.BuildRepository.CreateEventSubTable())
            {
                this.subTables = (await this.BuildRepository.GetSubTables()).OrderBy(table => table.EndTime).ToList();
            }

            await this.BuildRepository.CreateSnapshotTable();
            await this.BuildRepository.CreateSnapshotArchiveTable();
            await this.BuildRepository.CreateEventArchiveTable();
        }

        public async ValueTask<List<EventSubTable>> GetSubTables()
        {
            var lastSubTable = this.subTables.LastOrDefault();
            if (lastSubTable is null || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
            {
                await this.semaphore.WaitAsync();
                try
                {
                    if (lastSubTable is null || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                    {
                        this.subTables = (await this.BuildRepository.GetSubTables()).OrderBy(table => table.EndTime).ToList();
                    }
                }
                finally
                {
                    this.semaphore.Release();
                }
            }

            return this.subTables;
        }

        public async ValueTask<EventSubTable> GetTable(long eventTimestamp)
        {
            var getTask = this.GetSubTables();
            if (!getTask.IsCompletedSuccessfully)
            {
                await getTask;
            }

            var subTable = this.SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
            if (subTable is null)
            {
                await this.semaphore.WaitAsync();
                subTable = this.SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
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
                                TableName = this.EventTable,
                                SubTable = $"{this.EventTable}_{index}",
                                Index = index,
                                StartTime = startTime,
                                EndTime = startTime + this.SubTableMillionSecondsInterval
                            };
                        }
                        else
                        {
                            var firstSubTable = getTask.Result.FirstOrDefault();
                            var index = firstSubTable.Index - 1;
                            subTable = new EventSubTable
                            {
                                TableName = this.EventTable,
                                SubTable = index < 0 ? $"{this.EventTable}_n_{index * -1}" : $"{this.EventTable}_{index}",
                                Index = index,
                                StartTime = firstSubTable.StartTime - this.SubTableMillionSecondsInterval,
                                EndTime = firstSubTable.StartTime
                            };
                        }

                        try
                        {
                            await this.BuildRepository.CreateEventTable(subTable);
                            this.subTables.Add(subTable);
                            subTable = this.subTables.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
                        }
                        catch (Exception ex)
                        {
                            this.logger.LogCritical(ex, this.serializer.Serialize(subTable));
                            subTable = default;
                            this.subTables = (await this.BuildRepository.GetSubTables()).OrderBy(table => table.EndTime).ToList();
                        }
                    }
                }
                finally
                {
                    this.semaphore.Release();
                }
            }

            if (subTable is null)
            {
                subTable = await this.GetTable(eventTimestamp);
            }

            return subTable;
        }
    }
}
