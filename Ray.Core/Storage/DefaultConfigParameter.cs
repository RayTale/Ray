namespace Ray.Core.Storage
{
    public class DefaultConfigParameter : IConfigParameter
    {
        public DefaultConfigParameter(bool singleton, bool isFollow, string followName = null)
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
