using System;
using System.Threading.Tasks;

namespace RushShopping.Repository
{
    public interface IGrainRepository<TEntity, in TPrimaryKey> : IDisposable where TEntity : class, IEntity<TPrimaryKey>
    {
        TEntity FirstOrDefault(TPrimaryKey id);

        Task<TEntity> FirstOrDefaultAsync(TPrimaryKey id);

        void Insert(TEntity entity);

        Task InsertAsync(TEntity entity);

        void Commit();

        Task CommitAsync();
    }
}