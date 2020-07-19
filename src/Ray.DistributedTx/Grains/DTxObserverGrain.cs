using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Ray.Core;
using Ray.Core.Event;
using Ray.DistributedTx.Configuration;

namespace Ray.DistributedTx
{
    public abstract class DTxObserverGrain<PrimaryKey, MainGrain> : ObserverGrain<PrimaryKey, MainGrain>
    {
        protected DistributedTxOptions TransactionOptions { get; private set; }

        protected override ValueTask DependencyInjection()
        {
            this.TransactionOptions = this.ServiceProvider.GetOptionsByName<DistributedTxOptions>(this.GrainType.FullName);
            return base.DependencyInjection();
        }

        protected override Task UnsafeTell(IEnumerable<FullyEvent<PrimaryKey>> eventList)
        {
            if (!this.TransactionOptions.RetainTxEvents)
            {
                return base.UnsafeTell(eventList.Where(e => !(e is TxCommitEvent)));
            }
            else
            {
                return base.UnsafeTell(eventList);
            }
        }
    }
}
