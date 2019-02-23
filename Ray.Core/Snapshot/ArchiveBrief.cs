using Ray.Core.Configuration;

namespace Ray.Core.Snapshot
{
    public class ArchiveBrief
    {
        public string Id { get; set; }
        public long StartVersion { get; set; }
        public long EndVersion { get; set; }
        public long StartTimestamp { get; set; }
        public long EndTimestamp { get; set; }
        public int Index { get; set; }
        /// <summary>
        /// 事件是否已经清理
        /// </summary>
        public bool EventIsCleared { get; set; }
        public bool IsCompletedArchive<GrainState>(ArchiveOptions<GrainState> archiveOptions, ArchiveBrief preArchive = default)
        {
            var intervalMilliseconds = preArchive == default ? EndTimestamp - StartTimestamp : EndTimestamp - preArchive.EndTimestamp;
            var intervalVersiion = EndVersion - StartVersion;
            return (intervalMilliseconds > archiveOptions.IntervalMilliSeconds && intervalVersiion > archiveOptions.IntervalVersion) ||
                intervalMilliseconds > archiveOptions.MaxIntervalMilliSeconds ||
                intervalVersiion > archiveOptions.MaxIntervalVersion;
        }
    }
}
