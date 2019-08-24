using Orleans;
using Ray.Core;
using Ray.Core.Event;
using Ray.Core.Snapshot;
using Ray.DistributedTransaction;
using Ray.DistributedTransaction.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Ray.DistributedTx.Grains
{
    public abstract class DTxShadowGrain<Main, PrimaryKey, StateType> : ShadowGrain<Main, PrimaryKey, StateType>
        where StateType : class, ICloneable<StateType>, new()
    {
        protected DistributedTxOptions TransactionOptions { get; private set; }
        protected override ValueTask DependencyInjection()
        {
            TransactionOptions = ServiceProvider.GetOptionsByName<DistributedTxOptions>(GrainType.FullName);
            return base.DependencyInjection();
        }
        protected override ValueTask Tell(IEnumerable<IFullyEvent<PrimaryKey>> eventList)
        {
            if (!TransactionOptions.RetainTxEvents)
                return base.Tell(eventList.Where(e => !(e is TxCommitEvent)));
            else
                return base.Tell(eventList);
        }
    }
}
