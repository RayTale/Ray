using Orleans;
using Ray.Core.EventSourcing;

namespace Ray.IGrains.Actors
{
    public interface IAccountFlow : IAsyncConcurrentGrain, IGrainWithIntegerKey
    {
    }
}
