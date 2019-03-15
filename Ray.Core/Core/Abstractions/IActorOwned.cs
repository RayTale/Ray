namespace Ray.Core
{
    public interface IActorOwned<PrimaryKey>
    {
        PrimaryKey StateId { get; set; }
    }
}
