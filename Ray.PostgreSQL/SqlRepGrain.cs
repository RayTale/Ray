using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.PostgreSQL
{
    public abstract class SqlRepGrain<K, S, W> : RepGrain<K, S, W>, ISqlGrain
    where S : class, IState<K>, new()
    where W : IMessageWrapper
    {
        public abstract SqlGrainConfig GrainConfig { get; }
    }
}
