using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dapper;

namespace Ray.PostgresqlES
{
    public class SqlTable
    {
        public string Connection { get; set; }
        public string EventTable { get; set; }
        public string SnapshotTable { get; set; }
        public string ToDbSnapshotTable { get; set; }
        bool sharding = false;
        int shardingDays;
        public SqlTable(string conn, string table, bool sharding = false, int shardingDays = 90)
        {
            Connection = conn;
            EventTable = table + "_event";
            SnapshotTable = table + "_state";
            ToDbSnapshotTable = table + "_todbstate";
            this.sharding = sharding;
            this.shardingDays = shardingDays;
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
            if (lastTable != null && !this.sharding) return lastTable;
            var subTime = eventTime.Subtract(startTime);
            var cVersion = subTime.TotalDays > 0 ? Convert.ToInt32(Math.Floor(subTime.TotalDays / shardingDays)) : 0;
            if (lastTable == null || cVersion > lastTable.Version)
            {
                lock (collectionLock)
                {
                    if (lastTable == null || cVersion > lastTable.Version)
                    {
                        TableInfo collection = new TableInfo();
                        collection.Version = cVersion;
                        collection.Prefix = EventTable;
                        collection.CreateTime = DateTime.UtcNow;
                        collection.Name = EventTable + "_" + cVersion;
                        try
                        {
                            CreateEventTable(collection);
                            lastTable = collection;
                        }
                        catch (Exception ex)
                        {
                            if (!(ex.InnerException is Npgsql.PostgresException e && e.SqlState == "23505"))
                            {
                                throw ex;
                            }
                            else
                            {
                                tableList = null;
                                return GetTable(eventTime).GetAwaiter().GetResult();
                            }
                        }
                    }
                }
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
        public void CreateEventTable(TableInfo table)
        {
            const string sql = @"
                    CREATE TABLE ""public"".""{0}"" (
                                    ""id"" varchar(30) COLLATE ""default"" NOT NULL,
                                    ""stateid"" varchar(30) COLLATE ""default"" NOT NULL,
                                    ""msgid"" varchar(30) COLLATE ""default"" NOT NULL,
                                    ""typecode"" varchar(100) COLLATE ""default"" NOT NULL,
                                    ""iscomplete"" bool NOT NULL,
                                    ""data"" bytea NOT NULL,
                                    ""version"" int8 NOT NULL
                                    )
                    WITH(OIDS= FALSE);
                            CREATE UNIQUE INDEX ""{0}_State_MsgId"" ON ""public"".""{0}"" USING btree(""stateid"", ""msgid"", ""typecode"");
                            CREATE UNIQUE INDEX ""{0}_State_Version"" ON ""public"".""{0}"" USING btree(""stateid"", ""version"");
                            ALTER TABLE ""public"".""{0}"" ADD PRIMARY KEY(""id"");";
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
    }
}
