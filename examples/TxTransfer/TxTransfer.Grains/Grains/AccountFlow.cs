using Ray.Core.Abstractions.Observer;
using Ray.Core.Event;
using Ray.DistributedTx;
using TxTransfer.Grains.Events;
using TxTransfer.IGrains;

namespace TxTransfer.Grains.Grains
{
    /// <summary>
    /// Processing the follow-up process
    /// Do not need to deal with any process can be deleted
    /// </summary>
    [EventIgnore(typeof(TopupEvent), typeof(TransferArrivedEvent), typeof(TransferDeductEvent))]
    [Observer(DefaultGroup.Primary, DefaultName.Flow, typeof(Account))]
    public sealed class AccountFlow : DTxObserverGrain<long, Account>, IAccountFlow
    {
    }
}
