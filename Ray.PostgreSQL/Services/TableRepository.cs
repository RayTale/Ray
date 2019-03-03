using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;
using Ray.Storage.PostgreSQL.Entitys;
using Ray.Storage.PostgreSQL.Services.Abstractions;

namespace Ray.Storage.PostgreSQL.Services
{
    public class TableRepository : ITableRepository
    {
        public async Task<bool> CreateSubRecordTable(string conn)
        {
            const string sql = @"
                    CREATE TABLE if not exists Ray_SubTable(
                        TableName varchar(255) not null,
                        SubTable varchar(255) not null,
                        Index int4 not null,
                        StartTime int8 not null,
                        EndTime int8 not null
                    )WITH (OIDS=FALSE);
                    CREATE UNIQUE INDEX IF NOT EXISTS subtable_record ON Ray_SubTable USING btree(TableName, Index)";
            using (var connection = SqlFactory.CreateConnection(conn))
            {
                return await connection.ExecuteAsync(sql) > 0;
            }
        }
        public async Task<List<SubTableInfo>> GetSubTableList(string conn, string tableName)
        {
            string sql = "SELECT * FROM Ray_SubTable where TableName=@TableName";
            using (var connection = SqlFactory.CreateConnection(conn))
            {
                return (await connection.QueryAsync<SubTableInfo>(sql, new { TableName = tableName })).AsList();
            }
        }
        public async Task CreateEventTable(string conn, SubTableInfo subTable, int stateIdLength)
        {
            const string sql = @"
                    create table {0} (
                            StateId varchar({1}) not null,
                            UniqueId varchar(250)  null,
                            TypeCode varchar(100)  not null,
                            Data jsonb not null,
                            Version int8 not null,
                            Timestamp int8 not null,
                            constraint {0}_id_unique unique(StateId,TypeCode,UniqueId)
                            ) WITH (OIDS=FALSE);
                            CREATE UNIQUE INDEX IF NOT EXISTS {0}_Version ON {0} USING btree(StateId, Version);";
            const string insertSql = "INSERT into Ray_SubTable  VALUES(@TableName,@SubTable,@Index,@StartTime,@EndTime)";
            using (var connection = SqlFactory.CreateConnection(conn))
            {
                await connection.OpenAsync();
                using (var trans = connection.BeginTransaction())
                {
                    try
                    {
                        await connection.ExecuteAsync(string.Format(sql, subTable.SubTable, stateIdLength), transaction: trans);
                        await connection.ExecuteAsync(insertSql, subTable, trans);
                        trans.Commit();
                    }
                    catch
                    {
                        trans.Rollback();
                        throw;
                    }
                }
            }
        }
        public async Task CreateEventArchiveTable(string conn, string tableName, int stateIdLength)
        {
            const string sql = @"
                    create table if not exists {0} (
                            StateId varchar({1}) not null,
                            UniqueId varchar(250)  null,
                            TypeCode varchar(100)  not null,
                            Data jsonb not null,
                            Version int8 not null,
                            Timestamp int8 not null,
                            constraint {0}_id_unique unique(StateId,TypeCode,UniqueId)
                            ) WITH (OIDS=FALSE);
                            CREATE UNIQUE INDEX IF NOT EXISTS {0}_Version ON {0} USING btree(StateId, Version);";
            using (var connection = SqlFactory.CreateConnection(conn))
            {
                await connection.ExecuteAsync(string.Format(sql, tableName, stateIdLength));
            }
        }

        public async Task CreateFollowSnapshotTable(string conn, string tableName, int stateIdLength)
        {
            const string sql = @"
                     CREATE TABLE if not exists {0}(
                     StateId varchar({1}) not null PRIMARY KEY,
                     StartTimestamp int8 not null,
                     Version int8 not null)WITH (OIDS=FALSE);";
            using (var connection = SqlFactory.CreateConnection(conn))
            {
                await connection.ExecuteAsync(string.Format(sql, tableName, stateIdLength));
            }
        }

        public async Task CreateSnapshotArchiveTable(string conn, string tableName, int stateIdLength)
        {
            const string sql = @"
                     CREATE TABLE if not exists {0}(
                     Id varchar(50) not null PRIMARY KEY,
                     StateId varchar({1}) not null,
                     StartVersion int8 not null,
                     EndVersion int8 not null,
                     StartTimestamp int8 not null,
                     EndTimestamp int8 not null,
                     Index int4 not null,
                     EventIsCleared bool not null,
                     Data jsonb not null,
                     IsOver bool not null,
                     Version int8 not null)WITH (OIDS=FALSE);
                     CREATE INDEX IF NOT EXISTS {0}_StateId ON {0} USING btree(StateId)";
            using (var connection = SqlFactory.CreateConnection(conn))
            {
                await connection.ExecuteAsync(string.Format(sql, tableName, stateIdLength));
            }
        }

        public async Task CreateSnapshotTable(string conn, string tableName, int stateIdLength)
        {
            const string sql = @"
                     CREATE TABLE if not exists {0}(
                     StateId varchar({1}) not null PRIMARY KEY,
                     Data jsonb not null,
                     Version int8 not null,
                     StartTimestamp int8 not null,
                     LatestMinEventTimestamp int8 not null,
                     IsLatest bool not null,
                     IsOver bool not null)WITH (OIDS=FALSE);";
            using (var connection = SqlFactory.CreateConnection(conn))
            {
                await connection.ExecuteAsync(string.Format(sql, tableName, stateIdLength));
            }
        }
    }
}
