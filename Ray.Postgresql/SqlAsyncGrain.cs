using Ray.Core;
using Ray.Core.EventSourcing;

namespace Ray.Postgresql
{
    public abstract class SqlAsyncGrain<K, S, W> : AsyncGrain<K, S, W>, ISqlGrain
    where S : class, IState<K>, new()
    where W : MessageWrapper
    {
        public abstract SqlGrainConfig GrainConfig { get; }
    }
}
