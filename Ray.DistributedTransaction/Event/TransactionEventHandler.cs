using Ray.Core;
using Ray.Core.Event;
using Ray.Core.Snapshot;

namespace Ray.DistributedTransaction
{
    public abstract class TransactionEventHandler<PrimaryKey, Snapshot> : IEventHandler<PrimaryKey, Snapshot>
          where Snapshot : class, new()
    {
        public void Apply(Snapshot<PrimaryKey, Snapshot> snapshot, IFullyEvent<PrimaryKey> fullyEvent)
        {
            switch (fullyEvent.Event)
            {
                case TransactionFinishEvent _:
                    {
                        snapshot.Base.ClearTransactionInfo(false);
                    }; break;
                case TransactionCommitEvent transactionCommitEvent:
                    {
                        snapshot.Base.TransactionStartVersion = transactionCommitEvent.StartVersion;
                        snapshot.Base.TransactionStartTimestamp = transactionCommitEvent.StartTimestamp;
                        snapshot.Base.TransactionId = transactionCommitEvent.Id;
                    }; break;
                default: CustomApply(snapshot, fullyEvent); break;
            }
        }
        public abstract void CustomApply(Snapshot<PrimaryKey, Snapshot> snapshot, IFullyEvent<PrimaryKey> fullyEvent);
    }
}
