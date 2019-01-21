using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IStateStorage<K, S, B>
        where S : IState<K, B>
        where B : IStateBase<K>, new()
    {
        Task<S> Get(K id);

        Task Insert(S data);

        Task Update(S data);

        Task Delete(K id);
        Task Over(K id);
    }
}
