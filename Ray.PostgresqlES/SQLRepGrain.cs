using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.PostgresqlES
{
    public abstract class SqlRepGrain<K, S, W> : ESRepGrain<K, S, W>, ISqlGrain
    where S : class, IState<K>, new()
    where W : MessageWrapper
    {
        public abstract SqlTable ESSQLTable { get; }
    }
}
