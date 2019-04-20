using System.Threading.Tasks;
using Ray.Core.Event;

namespace RushShopping.Grains
{
    public interface ICrudHandle<in TSnapshot> where TSnapshot : class, new()
    {
        Task Apply(TSnapshot snapshot, IEvent evt);
    }
}