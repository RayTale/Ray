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
            this.TransactionOptions = this.ServiceProvider.GetOptionsByName<DistributedTxOptions>(this.GrainType.FullName);
            return base.DependencyInjection();
        }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            if (!(this.SnapshotHandler is DTxSnapshotHandler<PrimaryKey, StateType>))
            {
                throw new SnapshotHandlerTypeException(this.SnapshotHandler.GetType().FullName);
            }
        }

        protected override ValueTask OnCommitTransaction(string transactionId)
        {
            // If it is a transaction with Id, join the transaction event and wait for Complete
            if (!string.IsNullOrEmpty(transactionId))
            {
                this.TxRaiseEvent(new TxCommitEvent(this.CurrentTransactionId, this.CurrentTransactionStartVersion + 1, this.WaitingForTransactionTransports.Min(t => t.FullyEvent.BasicInfo.Timestamp)));
            }

            return Consts.ValueTaskDone;
        }

        protected override async ValueTask OnFinshTransaction(string transactionId)
        {
            if (!string.IsNullOrEmpty(transactionId))
            {
                if (!this.TransactionOptions.RetainTxEvents)
                {
                    if (this.Snapshot.Base is TxSnapshotBase<PrimaryKey> snapshotBase &&
                        this.BackupSnapshot.Base is TxSnapshotBase<PrimaryKey> backupSnapshotBase)
                    {
                        // Delete the last TransactionCommitEvent
                        await this.EventStorage.DeleteByVersion(this.Snapshot.Base.StateId, snapshotBase.TransactionStartVersion, snapshotBase.TransactionStartTimestamp);
                        var txCommitEvent = this.WaitingForTransactionTransports.Single(o => o.FullyEvent.BasicInfo.Version == snapshotBase.TransactionStartVersion);
                        this.WaitingForTransactionTransports.Remove(txCommitEvent);
                        snapshotBase.ClearTransactionInfo(true);
                        backupSnapshotBase.ClearTransactionInfo(true);
                    }
                    else
                    {
                        throw new SnapshotNotSupportTxException(this.Snapshot.GetType());
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
