namespace Ray.Storage.MongoDB
{
    public class ConfigParameter
    {
        public ConfigParameter(bool singleton, bool isFollow, string followName = null)
        {
            Singleton = singleton;
            IsFollow = isFollow;
            FollowName = followName;
        }
        public bool Singleton { get; }
        public bool IsFollow { get; }
        public string FollowName { get; set; }
    }
}
