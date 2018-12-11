using System.Threading.Tasks;

namespace Ray.Core.Internal
{
    public interface IFollowGrain
    {
        Task Tell(byte[] bytes);
    }
}
