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

        Task Delete(K id);
        Task Over(K id);
    }
}
