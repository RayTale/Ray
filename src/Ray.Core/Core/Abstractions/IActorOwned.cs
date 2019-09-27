namespace Ray.Core.Abstractions
{
    public interface IActorOwned<PrimaryKey>
    {
        PrimaryKey StateId { get; set; }
    }
}
