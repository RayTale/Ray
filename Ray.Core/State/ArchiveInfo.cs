namespace Ray.Core.State
{
    public class ArchiveInfo
    {
        long Id { get; set; }
        long StartVersion { get; set; }
        long EndVersion { get; set; }
        long StartTimestamp { get; set; }
        long EndTimestamp { get; set; }
        public int Index { get; set; }
    }
}
