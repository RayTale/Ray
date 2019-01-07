namespace Ray.Core.State
{
    public interface IStateOwned<K>
    {
        K StateId { get; set; }
    }
}
