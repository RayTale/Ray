using System;
using System.Threading.Tasks;
using Ray.Core.Abstractions;
using Ray.Core.Messaging;

namespace Ray.Core.EventBus
{
    public abstract class MultiSubtHandler<K, TMessageWrapper> : SubHandler<TMessageWrapper>
        where TMessageWrapper : IBytesWrapper
    {
        public MultiSubtHandler(IServiceProvider svProvider) : base(svProvider)
        {
        }
        public override Task Tell(byte[] wrapBytes, byte[] dataBytes, object data, TMessageWrapper msg)
        {
            return data is IEventBase<K> evt
                ? Task.WhenAll(SendToAsyncGrain(wrapBytes, evt), LocalProcess(dataBytes, data, msg))
                : LocalProcess(dataBytes, data, msg);
        }
        protected abstract Task SendToAsyncGrain(byte[] bytes, IEventBase<K> evt);
        public virtual Task LocalProcess(byte[] dataBytes, object data, TMessageWrapper msg) => Task.CompletedTask;
    }
}
