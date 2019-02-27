using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class StorageConfig : IStorageConfig
    {
        public string Connection { get; set; }
        public string EventTable { get; set; }
        public string SnapshotTable { get; set; }
        public string FollowName { get; set; }
        public bool IsFollow { get; set; }
        public string ArchiveSnapshotTable => $"{SnapshotTable}_Archive";
        public string FollowSnapshotTable => $"{SnapshotTable}_{FollowName}";
        public List<TableInfo> AllSplitTableList { get; set; }
        public int StateIdLength { get; }
        readonly bool sharding = false;
        readonly long shardingMinutes;
        public TableRepository TableRepository { get; }
        public bool Singleton { get; set; }

        public StorageConfig(string conn, string eventTable, string snapshotTable, bool isFollow = false, string followName = null, bool sharding = true, int shardingDays = 40, int stateIdLength = 200)
        {
            Connection = conn;
            EventTable = eventTable;
            SnapshotTable = snapshotTable;
            FollowName = followName;
            this.sharding = sharding;
            IsFollow = isFollow;
            shardingMinutes = shardingDays * 24 * 60;
            StateIdLength = stateIdLength;
            TableRepository = new TableRepository(this);
        }
        int isBuilded = 0;
        bool buildedResult = false;
        public async ValueTask Init()
        {
            while (!buildedResult)
            {
                if (Interlocked.CompareExchange(ref isBuilded, 1, 0) == 0)
                {
                    try
                    {
                        if (!IsFollow)
                        {
                            await TableRepository.CreateSubRecordTable();
                            await TableRepository.CreateStateTable();
                            await TableRepository.CreateArchiveStateTable();
                        }
                        else if (!string.IsNullOrEmpty(FollowName))
                        {
                            await TableRepository.CreateFollowStateTable();
                        }
                        AllSplitTableList = await TableRepository.GetTableListFromDb();
                        buildedResult = true;
                    }
                    finally
                    {
                        Interlocked.Exchange(ref isBuilded, 0);
                    }
                }
                await Task.Delay(50);
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DbConnection CreateConnection()
        {
            return SqlFactory.CreateConnection(Connection);
        }
        public int GetVersion(long eventTimestamp)
        {
            var firstTable = AllSplitTableList.FirstOrDefault();
            //如果不需要分表，直接返回
            if (firstTable != null && !sharding) return 0;
            var nowUtcTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var subMinutes = (eventTimestamp - (firstTable != null ? firstTable.CreateTime : nowUtcTime)) / (1000 * 60);
            return subMinutes > 0 ? (int)(subMinutes / shardingMinutes) : 0;
        }
        public async ValueTask<TableInfo> GetTable(long eventTimestamp)
        {
            var firstTable = AllSplitTableList.FirstOrDefault();
            //如果不需要分表，直接返回
            if (firstTable != null && !sharding) return firstTable;
            var nowUtcTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var subMinutes = (eventTimestamp - (firstTable != null ? firstTable.CreateTime : nowUtcTime)) / (60 * 1000);
            var version = subMinutes > 0 ? (int)(subMinutes / shardingMinutes) : 0;
            var resultTable = AllSplitTableList.FirstOrDefault(t => t.Version == version);
            if (resultTable == default)
            {
                var table = new TableInfo
                {
                    Version = version,
                    Prefix = EventTable,
                    CreateTime = nowUtcTime,
                    Name = EventTable + "_" + version
                };
                try
                {
                    await TableRepository.CreateEventTable(table);
                    AllSplitTableList.Add(table);
                    resultTable = table;
                }
                catch (Exception ex)
                {
                    AllSplitTableList = await TableRepository.GetTableListFromDb();
                    if (ex is Npgsql.PostgresException e && e.SqlState != "42P07" && e.SqlState != "23505")
                    {
                        throw;
                    }
                    resultTable = AllSplitTableList.First(t => t.Version == version);
                }
            }
            return resultTable;
        }
    }
}
