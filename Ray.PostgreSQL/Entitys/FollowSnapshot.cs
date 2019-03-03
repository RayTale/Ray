namespace Ray.Storage.PostgreSQL
{
    public class FollowSnapshot
    {
        public string StateId { get; set; }
        public long Version { get; set; }
        public long StartTimestamp { get; set; }
    }
}
