namespace Ray.Core.Messaging
{
    public interface IStateMessage<K>
    {
        K StateId { get; set; }
    }
}
