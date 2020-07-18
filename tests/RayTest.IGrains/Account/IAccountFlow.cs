using Orleans;
using Ray.Core.Observer;

namespace RayTest.IGrains
{
    public interface IAccountFlow : IObserver, IGrainWithIntegerKey
    {
    }
}
