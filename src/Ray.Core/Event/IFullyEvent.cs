using Ray.Core.Abstractions;

namespace Ray.Core.Event
{
    public interface IFullyEvent<PrimaryKey> : IActorOwned<PrimaryKey>
    {
        IEvent Event { get; set; }
        EventBase Base { get; set; }
    }
}
