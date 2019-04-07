using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Ray.Core;
using Ray.Core.Exceptions;
using Ray.Core.Snapshot;
using Ray.DistributedTransaction.Configuration;

namespace Ray.DistributedTransaction
{
    public abstract class DistributedTxGrain<Grain, PrimaryKey, StateType> : ConcurrentTxGrain<Grain, PrimaryKey, StateType>
        where StateType : class, ICloneable<StateType>, new()
    {
        public DistributedTxGrain() : base()
        {
        }
        protected DistributedTxOptions TransactionOptions { get; private set; }
        protected override ValueTask DependencyInjection()
        {
            TransactionOptions = ServiceProvider.GetOptionsByName<DistributedTxOptions>(GrainType.FullName);
            return base.DependencyInjection();
        }
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            if (!(EventHandler is TxEventHandler<PrimaryKey, StateType>))
            {
                throw new EventHandlerTypeException(EventHandler.GetType().FullName);
            }
        }
        protected override ValueTask OnCommitTransaction(long transactionId)
        {
            //如果是带Id的事务，则加入事务事件，等待Complete
            if (transactionId > 0)
            {
                TxRaiseEvent(new TxCommitEvent(CurrentTransactionId, CurrentTransactionStartVersion + 1, WaitingForTransactionTransports.Min(t => t.FullyEvent.Base.Timestamp)));
            }
            return Consts.ValueTaskDone;
        }
        protected override async ValueTask OnFinshTransaction(long transactionId)
        {
            if (transactionId > 0)
            {
                if (!TransactionOptions.RetainTxEvents)
                {
                    //删除最后一个TransactionCommitEvent
                    await EventStorage.DeleteEnd(Snapshot.Base.StateId, Snapshot.Base.Version, Snapshot.Base.LatestMinEventTimestamp);
                    if (Snapshot.Base is TxSnapshotBase<PrimaryKey> snapshotBase &&
                        BackupSnapshot.Base is TxSnapshotBase<PrimaryKey> backupSnapshotBase)
                    {
                        snapshotBase.ClearTransactionInfo(true);
                        backupSnapshotBase.ClearTransactionInfo(true);
                    }
                    else
                    {
                        throw new SnapshotNotSupportTxException(Snapshot.GetType());
                    }
                }
                else
                {
                    await base.RaiseEvent(new TxFinishedEvent(transactionId));
                }
            }
        }
    }
}
