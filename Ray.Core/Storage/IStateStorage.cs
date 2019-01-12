using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IStateStorage<K, S>
        where S : IActorState<K>
    {
        Task<S> Get(K id);

        Task Insert(S data);

        Task Update(S data);

        Task Delete(K id);
    }
}
