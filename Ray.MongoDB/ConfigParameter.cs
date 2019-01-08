namespace Ray.Storage.MongoDB
{
    public class ConfigParameter
    {
        public ConfigParameter(bool staticByType, string snapshotCollection)
        {
            StaticByType = staticByType;
            SnapshotCollection = snapshotCollection;
        }
        public bool StaticByType { get; }
        public string SnapshotCollection { get; }
    }
}
