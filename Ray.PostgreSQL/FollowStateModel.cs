namespace Ray.Storage.PostgreSQL
{
    public class FollowStateModel
    {
        public string StateId { get; set; }
        public long DoingVersion { get; set; }
        public long Version { get; set; }
    }
}
