using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dapper;

namespace Ray.Postgresql
{
    public class SqlGrainConfig
    {
        public string Connection { get; set; }
        public string EventTable { get; set; }
        public string SnapshotTable { get; set; }
        bool sharding = false;
        int shardingDays;
        public SqlGrainConfig(string conn, string eventTable, string snapshotTable, bool sharding = false, int shardingDays = 90)
        {
            Connection = conn;
            EventTable = eventTable;
            SnapshotTable = snapshotTable;
            this.sharding = sharding;
            this.shardingDays = shardingDays;
            CreateStateTable();
        }

        object collectionLock = new object();
        static DateTime startTime = new DateTime(2018, 1, 30);
        public async Task<List<TableInfo>> GetTableList(DateTime? startTime = null)
        {
            List<TableInfo> list = null;
            if (startTime == null)
                list = await GetTableList();
            else
            {
                var table = await GetTable(startTime.Value);
                list = (await GetTableList()).Where(c => c.Version >= table.Version).ToList();
            }
            if (list == null)
            {
                list = new List<TableInfo>() { await GetTable(DateTime.UtcNow) };
            }
            return list;
        }
        public async Task<TableInfo> GetTable(DateTime eventTime)
        {
            TableInfo lastTable = null;
            var cList = await GetTableList();
            if (cList.Count > 0) lastTable = cList.Last();
            //如果不需要分表，直接返回
            if (lastTable != null && !sharding) return lastTable;
            var subTime = eventTime.Subtract(startTime);
            var cVersion = subTime.TotalDays > 0 ? Convert.ToInt32(Math.Floor(subTime.TotalDays / shardingDays)) : 0;
            if (lastTable == null || cVersion > lastTable.Version)
            {
                lock (collectionLock)
                {
                    if (lastTable == null || cVersion > lastTable.Version)
                    {
                        TableInfo table = new TableInfo
                        {
                            Version = cVersion,
                            Prefix = EventTable,
                            CreateTime = DateTime.UtcNow,
                            Name = EventTable + "_" + cVersion
                        };
                        try
                        {
                            CreateEventTable(table);
                            lastTable = table;
                        }
                        catch (Exception ex)
                        {
                            if (ex is Npgsql.PostgresException e && e.SqlState != "42P07")
                            {
                                throw ex;
                            }
                            else
                            {
                                tableList = null;
                                lastTable = null;
                            }
                        }
                    }
                }
                if (lastTable == null)
                    return await GetTable(eventTime);
            }
            return lastTable;
        }
        private List<TableInfo> tableList;
        const string sql = "SELECT * FROM ray_tablelist where prefix=@Table order by version asc";
        public async Task<List<TableInfo>> GetTableList()
        {
            if (tableList == null)
            {
                using (var connection = SqlFactory.CreateConnection(Connection))
                {
                    tableList = (await connection.QueryAsync<TableInfo>(sql, new { Table = EventTable })).AsList();
                }
            }
            return tableList;
        }
        public DbConnection CreateConnection()
        {
            return SqlFactory.CreateConnection(Connection);
        }
        private void CreateEventTable(TableInfo table)
        {
            const string sql = @"
                    CREATE TABLE ""public"".""{0}"" (
                                    ""id"" varchar(30) COLLATE ""default"" NOT NULL PRIMARY KEY,
                                    ""stateid"" varchar(30) COLLATE ""default"" NOT NULL,
                                    ""msgid"" varchar(30) COLLATE ""default"" NOT NULL,
                                    ""typecode"" varchar(100) COLLATE ""default"" NOT NULL,
                                    ""data"" bytea NOT NULL,
                                    ""version"" int8 NOT NULL
                                    )
                            WITH (OIDS=FALSE);                      
                            CREATE UNIQUE INDEX ""Event_State_MsgId"" ON ""public"".""{0}"" USING btree (""stateid"", ""msgid"", ""typecode"");
                            CREATE UNIQUE INDEX ""Event_State_Version"" ON ""public"".""{0}"" USING btree(""stateid"", ""version"");";
            const string insertSql = "INSERT into ray_tablelist  VALUES(@Prefix,@Name,@Version,@CreateTime)";
            using (var connection = SqlFactory.CreateConnection(Connection))
            {
                connection.Open();
                using (var trans = connection.BeginTransaction())
                {
                    try
                    {
                        connection.Execute(string.Format(sql, table.Name), transaction: trans);
                        connection.Execute(insertSql, new { table.Prefix, table.Name, table.Version, table.CreateTime }, trans);
                        trans.Commit();
                    }
                    catch (Exception e)
                    {
                        trans.Rollback();
                        throw e;
                    }
                }
            }
        }

        private void CreateStateTable()
        {
            const string sql = @"
                    CREATE TABLE if not exists ""public"".""{0}""(
                    ""stateid"" varchar(30) COLLATE ""default"" NOT NULL PRIMARY KEY,
                    ""data"" bytea NOT NULL)";
            using (var connection = SqlFactory.CreateConnection(Connection))
            {
                connection.Open();
                connection.Execute(string.Format(sql, SnapshotTable));
            }
        }
    }
}
