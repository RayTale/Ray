using System.Threading.Tasks;

namespace Ray.Core
{
    public interface IFollow
    {
        Task Tell(byte[] bytes);
    }
}
