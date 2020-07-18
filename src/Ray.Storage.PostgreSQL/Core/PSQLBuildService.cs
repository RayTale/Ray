﻿using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;
using Ray.Storage.SQLCore;
using Ray.Storage.SQLCore.Configuration;
using Ray.Storage.SQLCore.Services;

namespace Ray.Storage.PostgreSQL
{
    public class PSQLBuildService : IBuildService
    {
        private readonly StringKeyOptions stringStorageOptions;
        private readonly StorageOptions storageOptions;
        private readonly bool stateIdIsString;
        public PSQLBuildService(StorageOptions storageOptions)
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
                    CREATE TABLE if not exists SubTable_Records(
                        TableName varchar(255) not null,
                        SubTable varchar(255) not null,
                        Index int4 not null,
                        StartTime int8 not null,
                        EndTime int8 not null
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS subtable_record ON SubTable_Records USING btree(TableName, Index)";
            using var connection = storageOptions.CreateConnection();
            return await connection.ExecuteAsync(sql) > 0;
        }
        public async Task CreateEventTable(EventSubTable subTable)
        {
            var stateIdSql = stateIdIsString ? $"StateId varchar({stringStorageOptions.StateIdLength}) not null" : "StateId int8 not null";
            var sql = $@"
                    create table {subTable.SubTable} (
                            {stateIdSql},
                            UniqueId varchar(250)  null,
                            TypeCode varchar(300)  not null,
                            Data json not null,
                            Version int8 not null,
                            Timestamp int8 not null,
                            constraint {subTable.SubTable}_id_unique unique(StateId,TypeCode,UniqueId)
                            );
                            CREATE UNIQUE INDEX IF NOT EXISTS {subTable.SubTable}_Version ON {subTable.SubTable} USING btree(StateId, Version);";
            const string insertSql = "INSERT into SubTable_Records  VALUES(@TableName,@SubTable,@Index,@StartTime,@EndTime)";
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
            var stateIdSql = stateIdIsString ? $"StateId varchar({stringStorageOptions.StateIdLength}) not null" : "StateId int8 not null";
            var sql = $@"
                    create table if not exists {storageOptions.EventArchiveTable} (
                            {stateIdSql},
                            UniqueId varchar(250)  null,
                            TypeCode varchar(300)  not null,
                            Data json not null,
                            Version int8 not null,
                            Timestamp int8 not null,
                            constraint {storageOptions.EventArchiveTable}_id_unique unique(StateId,TypeCode,UniqueId)
                            );
                            CREATE UNIQUE INDEX IF NOT EXISTS {storageOptions.EventArchiveTable}_Version ON {storageOptions.EventArchiveTable} USING btree(StateId, Version);";
            using var connection = storageOptions.CreateConnection();
            await connection.ExecuteAsync(sql);
        }

        public async Task CreateObserverSnapshotTable(string observerSnapshotTable)
        {
            var stateIdSql = stateIdIsString ? $"StateId varchar({stringStorageOptions.StateIdLength}) not null PRIMARY KEY" : "StateId int8 not null PRIMARY KEY";
            var sql = $@"
                     CREATE TABLE if not exists {observerSnapshotTable}(
                     {stateIdSql},
                     StartTimestamp int8 not null,
                     Version int8 not null);";
            using var connection = storageOptions.CreateConnection();
            await connection.ExecuteAsync(sql);
        }

        public async Task CreateSnapshotArchiveTable()
        {
            var stateIdSql = stateIdIsString ? $"StateId varchar({stringStorageOptions.StateIdLength}) not null" : "StateId int8 not null";
            var sql = $@"
                     CREATE TABLE if not exists {storageOptions.SnapshotArchiveTable}(
                     Id varchar(50) not null PRIMARY KEY,
                     {stateIdSql},
                     StartVersion int8 not null,
                     EndVersion int8 not null,
                     StartTimestamp int8 not null,
                     EndTimestamp int8 not null,
                     Index int4 not null,
                     EventIsCleared bool not null,
                     Data json not null,
                     IsOver bool not null,
                     Version int8 not null);
                     CREATE INDEX IF NOT EXISTS {storageOptions.SnapshotArchiveTable}_StateId ON {storageOptions.SnapshotArchiveTable} USING btree(StateId)";
            using var connection = storageOptions.CreateConnection();
            await connection.ExecuteAsync(sql);
        }

        public async Task CreateSnapshotTable()
        {
            var stateIdSql = stateIdIsString ? $"StateId varchar({stringStorageOptions.StateIdLength}) not null PRIMARY KEY" : "StateId int8 not null PRIMARY KEY";
            var sql = $@"
                     CREATE TABLE if not exists {storageOptions.SnapshotTable}(
                     {stateIdSql},
                     Data json not null,
                     Version int8 not null,
                     StartTimestamp int8 not null,
                     LatestMinEventTimestamp int8 not null,
                     IsLatest bool not null,
                     IsOver bool not null);";
            using var connection = storageOptions.CreateConnection();
            await connection.ExecuteAsync(sql);
        }
    }
}
