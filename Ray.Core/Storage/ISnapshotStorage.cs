using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface ISnapshotStorage<K, S, B>
        where S : IState<K, B>
        where B : ISnapshot<K>, new()
    {
        Task<S> Get(K id);

        Task Insert(S data);

        Task Update(S data);
        Task UpdateLatestMinEventTimestamp(K id, long timestamp);

        Task Delete(K id);
        /// <summary>
        /// 标记状态对应的Grain已经结束，需要设置状态的IsLatest=true
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Task Over(K id);
    }
}
