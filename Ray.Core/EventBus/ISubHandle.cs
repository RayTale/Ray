using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface ISubHandler
    {
        Task Notice(byte[] data);
    }
}
