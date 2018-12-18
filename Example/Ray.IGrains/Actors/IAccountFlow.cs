using Orleans;
using Ray.Core.Internal;

namespace Ray.IGrains.Actors
{
    public interface IAccountFlow : IConcurrentFollowGrain, IGrainWithIntegerKey
    {
    }
}
