using Orleans;
using Ray.Core.Abstractions;

namespace Ray.IGrains.Actors
{
    public interface IAccountFlow : IConcurrentFollow, IGrainWithIntegerKey
    {
    }
}
