using System;
using System.Threading.Tasks;
using Ray.Core.EventSourcing;
using Ray.Core.Message;

namespace Ray.Core.MQ
{
    public abstract class MultHandler<K, TMessageWrapper> : SubHandler<TMessageWrapper>
        where TMessageWrapper : MessageWrapper
    {
        public MultHandler(IServiceProvider svProvider) : base(svProvider)
        {
        }
        public override Task Tell(byte[] wrapBytes, byte[] dataBytes, IMessage data, TMessageWrapper msg)
        {
            if (data is IEventBase<K> evt)
            {
                return Task.WhenAll(SendToAsyncGrain(wrapBytes, evt), LocalProcess(dataBytes, data, msg));
            }
            else
            {
                return LocalProcess(dataBytes, data, msg);
            }
        }
        protected abstract Task SendToAsyncGrain(byte[] bytes, IEventBase<K> evt);
        public virtual Task LocalProcess(byte[] dataBytes, IMessage data, TMessageWrapper msg) => Task.CompletedTask;
    }
}
