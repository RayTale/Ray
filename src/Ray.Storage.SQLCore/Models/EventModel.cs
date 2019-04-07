namespace Ray.Storage.SQLCore
{
    public class EventModel
    {
        public string TypeCode { get; set; }
        public string Data { get; set; }
        public long Version { get; set; }
        public long Timestamp { get; set; }
    }
}
