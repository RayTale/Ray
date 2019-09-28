using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Snapshot;

namespace Ray.DistributedTx
{
    public abstract class DTxSnapshotHandler<PrimaryKey, Snapshot> : SnapshotHandler<PrimaryKey, Snapshot>
          where Snapshot : class, new()
    {
        public override void Apply(Snapshot<PrimaryKey, Snapshot> snapshot, FullyEvent<PrimaryKey> fullyEvent)
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
                default:
                    {
                        //如果产生非事务相关的事件，说明事务事件已被清理，应该执行一次清理动作
                        if (snapshot.Base is TxSnapshotBase<PrimaryKey> snapshotBase && snapshotBase.TransactionStartVersion != -1)
                        {
                            snapshotBase.ClearTransactionInfo(false);
                        }
                        CustomApply(snapshot, fullyEvent);
                    }; break;
            }
        }
        public virtual void CustomApply(Snapshot<PrimaryKey, Snapshot> snapshot, FullyEvent<PrimaryKey> fullyEvent)
        {
            base.Apply(snapshot, fullyEvent);
        }
    }
}
