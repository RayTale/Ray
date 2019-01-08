namespace Ray.Storage.PostgreSQL
{
    public class ConfigParameter
    {
        public ConfigParameter(bool staticByType, string snapshotTable)
        {
            StaticByType = staticByType;
            SnapshotTable = snapshotTable;
        }
        public bool StaticByType { get; }
        public string SnapshotTable { get; }
    }
}
