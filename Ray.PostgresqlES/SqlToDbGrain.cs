using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.PostgresqlES
{
    public abstract class SqlToDbGrain<K, W> : ToDbGrain<K, ToDbState<K>, W>, ISqlGrain
        where W : MessageWrapper
    {
        public abstract SqlTable ESSQLTable { get; }
    }
}
