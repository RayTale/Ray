using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface IConsumer
    {
        Task Notice(BytesBox bytes);

        Task Notice(List<BytesBox> list);
    }
}
