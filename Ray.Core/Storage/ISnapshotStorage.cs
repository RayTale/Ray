using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface ISnapshotStorage<K, S>
        where S : class, new()
    {
        Task<Snapshot<K, S>> Get(K id);

        Task Insert(Snapshot<K, S> data);

        Task Update(Snapshot<K, S> data);
        Task UpdateLatestMinEventTimestamp(K id, long timestamp);
        Task UpdateStartTimestamp(K id, long timestamp);
        Task UpdateIsLatest(K id, bool isLatest);

        Task Delete(K id);
        /// <summary>
        /// 标记状态对应的Grain已经结束，需要设置状态的IsLatest=true
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Task Over(K id, bool isOver);
    }
}
