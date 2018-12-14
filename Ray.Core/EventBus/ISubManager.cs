using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface ISubManager
    {
        Task Start(List<Subscriber> subscribers, string group, string node = null, List<string> nodeList = null);
        void Stop();
    }
}
