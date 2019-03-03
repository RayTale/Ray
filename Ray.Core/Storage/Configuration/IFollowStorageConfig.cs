using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IFollowStorageConfig
    {
        IStorageConfig Config { get; set; }
        string FollowName { get; set; }
        ValueTask Init();
    }
}
