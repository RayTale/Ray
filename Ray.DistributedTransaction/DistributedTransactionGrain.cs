using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Ray.Core;
using Ray.Core.Snapshot;
using Ray.DistributedTransaction.Configuration;

namespace Ray.DistributedTransaction
{
    public abstract class DistributedTransactionGrain<Grain, PrimaryKey, StateType> : ConcurrentTxGrain<Grain, PrimaryKey, StateType>
        where StateType : class, ICloneable<StateType>, new()
    {
        public DistributedTransactionGrain() : base()
        {
        }
        protected DistributedTransactionOptions TransactionOptions { get; private set; }
        protected override ValueTask DependencyInjection()
        {
            TransactionOptions = ServiceProvider.GetOptionsByName<DistributedTransactionOptions>(GrainType.FullName);
            return base.DependencyInjection();
        }
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            if (!(EventHandler is TransactionEventHandler<PrimaryKey, StateType>))
            {
                throw new TypeErrorOfEventHandlerException(EventHandler.GetType().FullName);
            }
        }
        protected override ValueTask OnCommitTransaction(long transactionId)
        {
            //如果是带Id的事务，则加入事务事件，等待Complete
            if (transactionId > 0)
            {
                TransactionRaiseEvent(new TransactionCommitEvent(CurrentTransactionId, CurrentTransactionStartVersion + 1, WaitingForTransactionTransports.Min(t => t.FullyEvent.Base.Timestamp)));
            }
            return Consts.ValueTaskDone;
        }
        protected override async ValueTask OnFinshTransaction(long transactionId)
        {
            if (transactionId > 0)
            {
                if (!TransactionOptions.RetainTransactionEvents)
                {
                    //删除最后一个TransactionCommitEvent
                    await EventStorage.DeleteEnd(Snapshot.Base.StateId, Snapshot.Base.Version, Snapshot.Base.LatestMinEventTimestamp);
                    Snapshot.Base.ClearTransactionInfo(true);
                    BackupSnapshot.Base.ClearTransactionInfo(true);
                }
                else
                {
                    await base.RaiseEvent(new TransactionFinishEvent(transactionId));
                }
            }
        }
    }
}
