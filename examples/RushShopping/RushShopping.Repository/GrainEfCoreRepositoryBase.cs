using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace RushShopping.Repository
{
    public class GrainEfCoreRepositoryBase<TEntity, TPrimaryKey> : IGrainRepository<TEntity, TPrimaryKey>
        where TEntity : class, IEntity<TPrimaryKey>
    {
        public GrainEfCoreRepositoryBase(RushShoppingDbContext context)
        {
            Context = context;
        }
        protected RushShoppingDbContext Context { get; set; }

        public virtual DbSet<TEntity> Table => Context.Set<TEntity>();

        #region Implementation of IDisposable

        public void Dispose()
        {
            if (IsDisposed)
            {
                return;
            }
            IsDisposed = true;
            Context.Dispose();
        }

        protected bool IsDisposed;

        #endregion

        public TEntity FirstOrDefault(TPrimaryKey id)
        {
            return Table.FirstOrDefault(CreateEqualityExpressionForId(id));
        }

        public Task<TEntity> FirstOrDefaultAsync(TPrimaryKey id)
        {
            return Table.FirstOrDefaultAsync(CreateEqualityExpressionForId(id));
        }

        protected virtual Expression<Func<TEntity, bool>> CreateEqualityExpressionForId(TPrimaryKey id)
        {
            var lambdaParam = Expression.Parameter(typeof(TEntity));

            var leftExpression = Expression.PropertyOrField(lambdaParam, "Id");

            Expression<Func<object>> closure = () => id;
            var rightExpression = Expression.Convert(closure.Body, leftExpression.Type);

            var lambdaBody = Expression.Equal(leftExpression, rightExpression);

            return Expression.Lambda<Func<TEntity, bool>>(lambdaBody, lambdaParam);
        }
    }
}