using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface IConsumer
    {
        Task Notice(byte[] bytes);
        Task Notice(List<byte[]> list);
    }
}
