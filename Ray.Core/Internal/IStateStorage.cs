using System.Threading.Tasks;
using System;

namespace Ray.Core.Internal
{
    public interface IStateStorage<T, K> where T : IState<K>
    {
        Task<T> GetByIdAsync(K id);

        Task InsertAsync(T data);

        Task UpdateAsync(T data);

        Task DeleteAsync(K id);
    }
}
