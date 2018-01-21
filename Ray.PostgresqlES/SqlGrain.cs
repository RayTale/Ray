using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.PostgresqlES
{
    public abstract class SqlGrain<K, S, W> : ESGrain<K, S, W>, ISqlGrain
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
