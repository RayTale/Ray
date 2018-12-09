using Orleans;
using System.Threading.Tasks;

namespace Ray.Core.MQ
{
    public interface IMQServiceContainer
    {
        ValueTask<IMQService> GetService(Grain grain);
    }
}
