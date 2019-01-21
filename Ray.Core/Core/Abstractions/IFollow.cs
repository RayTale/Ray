using System.Threading.Tasks;

namespace Ray.Core
{
    public interface IFollow : IVersionGrain
    {
        Task Tell(byte[] bytes);
    }
}
