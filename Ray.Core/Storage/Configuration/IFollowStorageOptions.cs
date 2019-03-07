using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IFollowStorageOptions
    {
        IStorageOptions Config { get; set; }
        string FollowName { get; set; }
        ValueTask Build();
    }
}
