using Orleans;
using Ray.Core;

namespace Ray.IGrains.Actors
{
    public interface IAccountFlow : IConcurrentObserver, IGrainWithIntegerKey
    {
    }
}
