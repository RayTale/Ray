using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Ray.Core;
using Ray.Core.Exceptions;
using Ray.Core.Snapshot;
using Ray.DistributedTx.Configuration;

namespace Ray.DistributedTx
{
    public abstract class DTxGrain<PrimaryKey, StateType> : ConcurrentTxGrain<PrimaryKey, StateType>
        where StateType : class, ICloneable<StateType>, new()
    {
        protected DistributedTxOptions TransactionOptions { get; private set; }
        protected override ValueTask DependencyInjection()
        {
            TransactionOptions = ServiceProvider.GetOptionsByName<DistributedTxOptions>(GrainType.FullName);
            return base.DependencyInjection();
        }
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            if (!(SnapshotHandler is DTxSnapshotHandler<PrimaryKey, StateType>))
            {
                throw new SnapshotHandlerTypeException(SnapshotHandler.GetType().FullName);
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
                    if (Snapshot.Base is TxSnapshotBase<PrimaryKey> snapshotBase &&
                        BackupSnapshot.Base is TxSnapshotBase<PrimaryKey> backupSnapshotBase)
                    {
                        //删除最后一个TransactionCommitEvent
                        await EventStorage.DeleteByVersion(Snapshot.Base.StateId, snapshotBase.TransactionStartVersion, snapshotBase.TransactionStartTimestamp);
                        var txCommitEvent = WaitingForTransactionTransports.Single(o => o.FullyEvent.Base.Version == snapshotBase.TransactionStartVersion);
                        WaitingForTransactionTransports.Remove(txCommitEvent);
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
