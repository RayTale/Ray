using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IStateStorage<K, T>
        where T : IActorState<K>
    {
        Task<T> GetByIdAsync(K id);

        Task InsertAsync(T data);

        Task UpdateAsync(T data);

        Task DeleteAsync(K id);
    }
}
