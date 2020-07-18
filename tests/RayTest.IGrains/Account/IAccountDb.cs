using Orleans;
using Ray.Core.Observer;

namespace RayTest.IGrains
{
    public interface IAccountDb : IObserver, IGrainWithIntegerKey
    {
    }
}
