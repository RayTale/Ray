using System.Threading.Tasks;

namespace Ray.Core.Abstractions
{
    public interface IFollow
    {
        Task Tell(byte[] bytes);
    }
}
