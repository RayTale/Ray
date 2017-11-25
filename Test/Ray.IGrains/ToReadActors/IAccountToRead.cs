using Coin.Core.EventSourcing;
using Orleans;

namespace Ray.IGrains.ToReadActors
{
    public interface IAccountToRead : IToRead<MessageInfo>, IGrainWithStringKey
    {
    }
}
