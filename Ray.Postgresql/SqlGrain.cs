using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.Postgresql
{
    public abstract class SqlGrain<K, S, W> : ESGrain<K, S, W>, ISqlGrain
    where S : class, IState<K>, new()
    where W : MessageWrapper
    {
        public abstract SqlGrainConfig ESSQLTable { get; }
    }
}
