using Ray.Core;
using Ray.Core.Event;
using Ray.Core.Observer;
using Ray.DistributedTx;
using Transfer.Grains.Events;
using Transfer.IGrains;

namespace Transfer.Grains.Grains
{
    /// <summary>
    /// 处理后续流程
    /// 不需要处理任何流程可以删除
    /// </summary>
    [Handler(typeof(TopupEvent), typeof(TransferArrivedEvent), typeof(TransferDeductEvent))]
    [Observer(DefaultObserverGroup.primary, "flow", typeof(Account))]
    public sealed class AccountFlow : DTxObserverGrain<long, Account>, IAccountFlow
    {
    }
}
