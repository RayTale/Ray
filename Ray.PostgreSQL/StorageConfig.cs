using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.Storage.PostgreSQL
{
    public class StorageConfig
    {
        public string Connection { get; set; }
        public string EventTable { get; set; }
        public string SnapshotTable { get; set; }
        public string FollowName { get; set; }
        public bool IsFollow { get; set; }
        public string ArchiveStateTable
        {
            get
            {
                return $"{SnapshotTable}_Archive";
            }
        }
        public List<TableInfo> AllSplitTableList { get; set; }
        public int StateIdLength { get; }
        readonly bool sharding = false;
        readonly long shardingMilliseconds;
        public TableRepository TableRepository { get; }
        public StorageConfig(string conn, string eventTable, string snapshotTable, bool isFollow = false, string followName = null, bool sharding = true, int shardingDays = 40, int stateIdLength = 200)
        {
            Connection = conn;
            EventTable = eventTable;
            SnapshotTable = snapshotTable;
            FollowName = followName;
            this.sharding = sharding;
            IsFollow = isFollow;
            shardingMilliseconds = shardingDays * 24 * 60 * 60 * 1000;
            StateIdLength = stateIdLength;
            TableRepository = new TableRepository(this);
        }
        public string GetFollowStateTable()
        {
            return $"{SnapshotTable}_{FollowName}";
        }
        int isBuilded = 0;
        bool buildedResult = false;
        public async ValueTask Build()
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
            var subMilliseconds = eventTimestamp - (firstTable != null ? firstTable.CreateTime : nowUtcTime);
            return subMilliseconds > 0 ? (int)(subMilliseconds / shardingMilliseconds) : 0;
        }
        public async ValueTask<TableInfo> GetTable(long eventTimestamp)
        {
            var firstTable = AllSplitTableList.FirstOrDefault();
            //如果不需要分表，直接返回
            if (firstTable != null && !sharding) return firstTable;
            var nowUtcTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var subMilliseconds = eventTimestamp - (firstTable != null ? firstTable.CreateTime : nowUtcTime);
            var version = subMilliseconds > 0 ? (int)(subMilliseconds / shardingMilliseconds) : 0;
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
