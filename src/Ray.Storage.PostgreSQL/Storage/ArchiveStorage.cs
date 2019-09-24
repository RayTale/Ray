using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;
using Ray.Storage.SQLCore;
using Ray.Storage.SQLCore.Configuration;

namespace Ray.Storage.PostgreSQL
{
    public class ArchiveStorage<PrimaryKey, StateType> : IArchiveStorage<PrimaryKey, StateType>
          where StateType : class, new()
    {
        readonly StorageOptions config;
        private readonly string deleteSql;
        private readonly string deleteAllSql;
        private readonly string getByIdSql;
        private readonly string getListByStateIdSql;
        private readonly string getLatestByStateIdSql;
        private readonly string insertSql;
        private readonly string updateOverSql;
        private readonly string updateEventIsClearSql;
        readonly ISerializer serializer;
        readonly ILogger<ArchiveStorage<PrimaryKey, StateType>> logger;
        public ArchiveStorage(IServiceProvider serviceProvider, ISerializer serializer, StorageOptions config)
        {
            logger = serviceProvider.GetService<ILogger<ArchiveStorage<PrimaryKey, StateType>>>();
            this.serializer = serializer;
            this.config = config;
            var tableName = config.SnapshotArchiveTable;
            deleteSql = $"DELETE FROM {tableName} where id=@Id";
            deleteAllSql = $"DELETE FROM {tableName} where stateid=@StateId";
            getByIdSql = $"select * FROM {tableName} where id=@Id";
            getListByStateIdSql = $"select Id,StartVersion,EndVersion,StartTimestamp,EndTimestamp,Index,EventIsCleared FROM {tableName} where stateid=@StateId";
            getLatestByStateIdSql = $"select Id,StartVersion,EndVersion,StartTimestamp,EndTimestamp,Index,EventIsCleared FROM {tableName} where stateid=@StateId order by index desc limit 1";
            insertSql = $"INSERT into {tableName}(Id,stateid,StartVersion,EndVersion,StartTimestamp,EndTimestamp,Index,EventIsCleared,data,IsOver,Version)VALUES(@Id,@StateId,@StartVersion,@EndVersion,@StartTimestamp,@EndTimestamp,@Index,@EventIsCleared,(@Data)::json,@IsOver,@Version)";
            updateOverSql = $"update {tableName} set IsOver=@IsOver where stateid=@StateId";
            updateEventIsClearSql = $"update {tableName} set EventIsCleared=true where id=@Id";
        }
        public async Task Delete(PrimaryKey stateId, string briefId)
        {
            using var conn = config.CreateConnection();
            await conn.ExecuteAsync(deleteSql, new { Id = briefId });
        }
        public async Task DeleteAll(PrimaryKey stateId)
        {
            using var conn = config.CreateConnection();
            await conn.ExecuteAsync(deleteAllSql, new { StateId = stateId });
        }
        public async Task EventIsClear(PrimaryKey stateId, string briefId)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(updateEventIsClearSql, new { Id = briefId });
        }

        public async Task<List<ArchiveBrief>> GetBriefList(PrimaryKey stateId)
        {
            using var connection = config.CreateConnection();
            return (await connection.QueryAsync<ArchiveBrief>(getListByStateIdSql, new { StateId = stateId })).AsList();
        }

        public async Task<ArchiveBrief> GetLatestBrief(PrimaryKey stateId)
        {
            using var connection = config.CreateConnection();
            return await connection.QuerySingleOrDefaultAsync<ArchiveBrief>(getLatestByStateIdSql, new { StateId = stateId });
        }

        public async Task<Snapshot<PrimaryKey, StateType>> GetById(string briefId)
        {
            using var connection = config.CreateConnection();
            var data = await connection.QuerySingleOrDefaultAsync<SnapshotModel<PrimaryKey>>(getByIdSql, new { Id = briefId });
            if (data != null)
            {
                return new Snapshot<PrimaryKey, StateType>()
                {
                    Base = new SnapshotBase<PrimaryKey>
                    {
                        StateId = data.StateId,
                        Version = data.Version,
                        DoingVersion = data.Version,
                        IsLatest = false,
                        IsOver = data.IsOver,
                        StartTimestamp = data.StartTimestamp,
                        LatestMinEventTimestamp = data.StartTimestamp
                    },
                    State = serializer.Deserialize<StateType>(data.Data)
                };
            }
            return default;
        }

        public async Task Insert(ArchiveBrief brief, Snapshot<PrimaryKey, StateType> snapshot)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(insertSql, new
            {
                brief.Id,
                snapshot.Base.StateId,
                brief.StartVersion,
                brief.EndVersion,
                brief.StartTimestamp,
                brief.EndTimestamp,
                brief.Index,
                brief.EventIsCleared,
                Data = serializer.Serialize(snapshot.State),
                snapshot.Base.IsOver,
                snapshot.Base.Version
            });
        }
        public async Task Over(PrimaryKey stateId, bool isOver)
        {
            using var connection = config.CreateConnection();
            await connection.ExecuteAsync(updateOverSql, new { StateId = stateId, IsOver = isOver });
        }
        public Task EventArichive(PrimaryKey stateId, long endVersion, long startTimestamp)
        {
            return Task.Run(async () =>
            {
                var getTableListTask = config.GetSubTables();
                if (!getTableListTask.IsCompletedSuccessfully)
                    await getTableListTask;
                var tableList = getTableListTask.Result.Where(t => t.EndTime >= startTimestamp);
                using var conn = config.CreateConnection();
                await conn.OpenAsync();
                using var trans = conn.BeginTransaction();
                try
                {
                    foreach (var table in tableList)
                    {
                        var copySql = $"insert into {config.EventArchiveTable} select * from {table.SubTable} WHERE stateid=@StateId and version<=@EndVersion";
                        var sql = $"delete from {table.SubTable} WHERE stateid=@StateId and version<=@EndVersion";
                        await conn.ExecuteAsync(copySql, new { StateId = stateId, EndVersion = endVersion }, transaction: trans);
                        await conn.ExecuteAsync(sql, new { StateId = stateId, EndVersion = endVersion }, transaction: trans);
                    }
                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans.Rollback();
                    logger.LogCritical(ex, ex.Message);
                    throw;
                }
            });
        }
    }
}
