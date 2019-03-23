using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Snapshot;

namespace Ray.DistributedTransaction
{
    public abstract class TxEventHandler<PrimaryKey, Snapshot> : IEventHandler<PrimaryKey, Snapshot>
          where Snapshot : class, new()
    {
        public void Apply(Snapshot<PrimaryKey, Snapshot> snapshot, IFullyEvent<PrimaryKey> fullyEvent)
        {
            switch (fullyEvent.Event)
            {
                case TxFinishedEvent _:
                    {
                        if (snapshot.Base is TxSnapshotBase<PrimaryKey> snapshotBase)
                            snapshotBase.ClearTransactionInfo(false);
                        else
                            throw new SnapshotNotSupportTxException(snapshot.GetType());
                    }; break;
                case TxCommitEvent transactionCommitEvent:
                    {
                        if (snapshot.Base is TxSnapshotBase<PrimaryKey> snapshotBase)
                        {
                            snapshotBase.TransactionStartVersion = transactionCommitEvent.StartVersion;
                            snapshotBase.TransactionStartTimestamp = transactionCommitEvent.StartTimestamp;
                            snapshotBase.TransactionId = transactionCommitEvent.Id;
                        }
                        else
                            throw new SnapshotNotSupportTxException(snapshot.GetType());
                    }; break;
                default: CustomApply(snapshot, fullyEvent); break;
            }
        }
        public abstract void CustomApply(Snapshot<PrimaryKey, Snapshot> snapshot, IFullyEvent<PrimaryKey> fullyEvent);
    }
}
