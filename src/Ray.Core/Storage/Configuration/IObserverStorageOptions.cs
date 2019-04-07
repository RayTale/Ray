using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IObserverStorageOptions
    {
        IStorageOptions Config { get; set; }
        string ObserverName { get; set; }
        ValueTask Build();
    }
}
