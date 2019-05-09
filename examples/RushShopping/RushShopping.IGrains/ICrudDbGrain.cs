using System.Threading.Tasks;
using Ray.Core.Event;

namespace RushShopping.IGrains
{
    public interface ICrudDbGrain<TPrimaryKey>
    {
        Task Process(IFullyEvent<TPrimaryKey> @event);
    }
}