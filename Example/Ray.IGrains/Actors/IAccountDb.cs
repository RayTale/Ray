using Orleans;
using Ray.Core.Internal;

namespace Ray.IGrains.Actors
{
    public interface IAccountDb : IConcurrentFollowGrain, IGrainWithIntegerKey
    {
    }
}
