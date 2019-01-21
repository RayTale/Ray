namespace Ray.Core
{
    public interface IActorOwned<K>
    {
        K StateId { get; set; }
    }
}
