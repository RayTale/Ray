namespace Ray.Storage.Mongo
{
    public class SubCollectionInfo
    {
        public string Table { get; set; }
        public string SubTable { get; set; }
        public int Index { get; set; }
        public long StartTime { get; set; }
        public long EndTime { get; set; }
    }
}
