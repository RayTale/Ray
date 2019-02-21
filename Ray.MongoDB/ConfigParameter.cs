using Ray.Core.Storage;

namespace Ray.Storage.MongoDB
{
    public class ConfigParameter : IStorageConfigParameter
    {
        public ConfigParameter(bool singleton, bool isFollow, string followName = null)
        {
            Singleton = singleton;
            IsFollow = isFollow;
            FollowName = followName;
        }
        public bool Singleton { get; set; }
        public bool IsFollow { get; }
        public string FollowName { get; set; }
    }
}
