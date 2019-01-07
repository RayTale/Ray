using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface IConsumerManager
    {
        Task Start();
        void Stop();
    }
}
