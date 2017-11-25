using System.Threading.Tasks;

namespace Ray.Core.MQ
{
    public interface ISubHandler
    {
        Task Notice(byte[] data);
    }
}
