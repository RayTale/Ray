namespace Ray.Core.Message
{
    public interface IActorOwnMessage<K> : IMessage
    {
        K StateId { get; set; }
    }
}
