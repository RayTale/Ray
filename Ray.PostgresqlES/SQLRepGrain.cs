using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.PostgresqlES
{
    public abstract class SqlRepGrain<K, S, W> : ESRepGrain<K, S, W>, ISqlGrain
    where S : class, IState<K>, new()
    where W : MessageWrapper
    {
        protected SqlTable _sqlTable = null;

        public virtual SqlTable ESSQLTable
        {
            get
            {
                return _sqlTable;
            }
        }
    }
}
