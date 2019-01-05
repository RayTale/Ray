using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface IConsumerManager
    {
        Task Start(string node, List<string> nodeList = null);
        void Stop();
    }
}
