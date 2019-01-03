namespace Ray.Core.Abstractions
{
    public interface IStateOwned<K>
    {
        K StateId { get; set; }
    }
}
