using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;
using Ray.Storage.SQLCore;
using Ray.Storage.SQLCore.Configuration;
using Ray.Storage.SQLCore.Services;

namespace Ray.Storage.MySQL
{
    public class MySQLBuildService : IBuildService
    {
        private readonly StringKeyOptions stringStorageOptions;
        private readonly StorageOptions storageOptions;
        private readonly bool stateIdIsString;
        public MySQLBuildService(StorageOptions storageOptions)
        {
            this.storageOptions = storageOptions;
            if (storageOptions is StringKeyOptions options)
            {
                stateIdIsString = true;
                stringStorageOptions = options;
            }
            else
            {
                stateIdIsString = false;
            }
        }
        public async Task<List<EventSubTable>> GetSubTables()
        {
            string sql = "SELECT * FROM SubTable_Records where TableName=@TableName";
            using var connection = storageOptions.CreateConnection();
            return (await connection.QueryAsync<EventSubTable>(sql, new { TableName = storageOptions.EventTable })).AsList();
        }
        public async Task<bool> CreateEventSubTable()
        {
            const string sql = @"
                                    CREATE TABLE if not exists `SubTable_Records`  (
                                      `TableName` varchar(255) NOT NULL,
                                      `SubTable` varchar(255) NOT NULL,
                                      `StartTime` bigint(20) NULL DEFAULT NULL,
                                      `EndTime` bigint(20) NULL DEFAULT NULL,
                                      `Index` int(255) NULL DEFAULT NULL,
                                      UNIQUE INDEX `SubTable_Records`(`TableName`, `Index`) USING BTREE
                                    );";
            using var connection = storageOptions.CreateConnection();
            return await connection.ExecuteAsync(sql) > 0;
        }
        public async Task CreateEventTable(EventSubTable subTable)
        {
            var stateIdSql = stateIdIsString ? $"`StateId` varchar({stringStorageOptions.StateIdLength}) NOT NULL" : "`StateId` int8 NOT NULL";
            var sql = $@"
                    create table if not exists `{subTable.SubTable}` (
                            {stateIdSql},
                            `UniqueId` varchar(250)  NULL DEFAULT NULL,
                            `TypeCode` varchar(100)  NOT NULL,
                            `Data` json NOT NULL,
                            `Version` int8 NOT NULL,
                            `Timestamp` int8 NOT NULL,
                            UNIQUE INDEX `id_unique`(`StateId`, `TypeCode`, `UniqueId`) USING BTREE,
                            UNIQUE INDEX `Version`(`StateId`, `Version`) USING BTREE
                            );";
            const string insertSql = "INSERT INTO SubTable_Records(TableName,SubTable,`Index`,StartTime,EndTime)  VALUES(@TableName,@SubTable,@Index,@StartTime,@EndTime)";
            using var connection = storageOptions.CreateConnection();
            await connection.OpenAsync();
            using var trans = connection.BeginTransaction();
            try
            {
                await connection.ExecuteAsync(sql, transaction: trans);
                await connection.ExecuteAsync(insertSql, subTable, trans);
                trans.Commit();
            }
            catch
            {
                trans.Rollback();
                throw;
            }
        }
        public async Task CreateEventArchiveTable()
        {
            var stateIdSql = stateIdIsString ? $"`StateId` varchar({stringStorageOptions.StateIdLength}) NOT NULL" : "`StateId` int8 NOT NULL";
            var sql = $@"
                    create table if not exists `{storageOptions.EventArchiveTable}` (
                            {stateIdSql},
                            `UniqueId` varchar(250)  null,
                            `TypeCode` varchar(100)  NOT NULL,
                            `Data` json NOT NULL,
                            `Version` int8 NOT NULL,
                            `Timestamp` int8 NOT NULL,
                            UNIQUE INDEX `id_unique`(`StateId`, `TypeCode`, `UniqueId`) USING BTREE,
                            UNIQUE INDEX `Version`(`StateId`, `Version`) USING BTREE
                            );";
            using var connection = storageOptions.CreateConnection();
            await connection.ExecuteAsync(sql);
        }

        public async Task CreateObserverSnapshotTable(string observerSnapshotTable)
        {
            var stateIdSql = stateIdIsString ? $"`StateId` varchar({stringStorageOptions.StateIdLength}) NOT NULL PRIMARY KEY" : "`StateId` int8 NOT NULL PRIMARY KEY";
            var sql = $@"
                     CREATE TABLE if not exists `{observerSnapshotTable}`(
                     {stateIdSql},
                     `StartTimestamp` int8 NOT NULL,
                     `Version` int8 NOT NULL);";
            using var connection = storageOptions.CreateConnection();
            await connection.ExecuteAsync(sql);
        }

        public async Task CreateSnapshotArchiveTable()
        {
            var stateIdSql = stateIdIsString ? $"`StateId` varchar({stringStorageOptions.StateIdLength}) NOT NULL" : "`StateId` int8 NOT NULL";
            var sql = $@"
                     CREATE TABLE if not exists `{storageOptions.SnapshotArchiveTable}`(
                     `Id` varchar(50) NOT NULL PRIMARY KEY,
                     {stateIdSql},
                     `StartVersion` int8 NOT NULL,
                     `EndVersion` int8 NOT NULL,
                     `StartTimestamp` int8 NOT NULL,
                     `EndTimestamp` int8 NOT NULL,
                     `Index` int4 NOT NULL,
                     `EventIsCleared` bool NOT NULL,
                     `Data` json NOT NULL,
                     `IsOver` bool NOT NULL,
                     `Version` int8 NOT NULL,
                     INDEX `StateId`(`StateId`) USING BTREE);";
            using var connection = storageOptions.CreateConnection();
            await connection.ExecuteAsync(sql);
        }

        public async Task CreateSnapshotTable()
        {
            var stateIdSql = stateIdIsString ? $"`StateId` varchar({stringStorageOptions.StateIdLength}) NOT NULL PRIMARY KEY" : "`StateId` int8 NOT NULL PRIMARY KEY";
            var sql = $@"
                     CREATE TABLE if not exists `{storageOptions.SnapshotTable}`(
                     {stateIdSql},
                     `Data` json NOT NULL,
                     `Version` int8 NOT NULL,
                     `StartTimestamp` int8 NOT NULL,
                     `LatestMinEventTimestamp` int8 NOT NULL,
                     `IsLatest` bool NOT NULL,
                     `IsOver` bool NOT NULL);";
            using var connection = storageOptions.CreateConnection();
            await connection.ExecuteAsync(sql);
        }
    }
}
