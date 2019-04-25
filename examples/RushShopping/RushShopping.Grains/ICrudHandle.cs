using System.Threading.Tasks;
using Ray.Core.Event;
using RushShopping.Grains.Events;

namespace RushShopping.Grains
{
    public interface ICrudHandle<TSnapshot> where TSnapshot : class, new()
    {
        void Apply(TSnapshot snapshot, IEvent evt);

        void CreatingSnapshotHandle(TSnapshot snapshotState, CreatingSnapshotEvent<TSnapshot> evt);
    }
}