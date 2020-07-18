using System.Threading.Tasks;
using Ray.Core.Snapshot;

namespace Ray.Core.Storage
{
    public interface ISnapshotStorage<PrimaryKey, StateType>
        where StateType : class, new()
    {
        Task<Snapshot<PrimaryKey, StateType>> Get(PrimaryKey id);

        Task Insert(Snapshot<PrimaryKey, StateType> snapshot);

        Task Update(Snapshot<PrimaryKey, StateType> snapshot);
        Task UpdateLatestMinEventTimestamp(PrimaryKey id, long timestamp);
        Task UpdateStartTimestamp(PrimaryKey id, long timestamp);
        Task UpdateIsLatest(PrimaryKey id, bool isLatest);

        Task Delete(PrimaryKey id);
        /// <summary>
        /// 标记状态对应的Grain已经结束，需要设置状态的IsLatest=true
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Task Over(PrimaryKey id, bool isOver);
    }
}
