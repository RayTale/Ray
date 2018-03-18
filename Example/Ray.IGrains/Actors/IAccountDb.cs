using Orleans;
using Ray.Core.EventSourcing;

namespace Ray.IGrains.Actors
{
    public interface IAccountDb : IAsyncGrain<MessageInfo>, IGrainWithStringKey
    {
    }
}
