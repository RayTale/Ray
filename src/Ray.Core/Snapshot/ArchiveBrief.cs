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
        public bool IsCompletedArchive(ArchiveOptions archiveOptions, ArchiveBrief preArchive = default)
        {
            var intervalMilliseconds = (preArchive is null ? EndTimestamp - StartTimestamp : EndTimestamp - preArchive.EndTimestamp) / 1000;
            var intervalVersiion = EndVersion - StartVersion;
            return (intervalMilliseconds > archiveOptions.SecondsInterval && intervalVersiion > archiveOptions.VersionInterval) ||
                intervalMilliseconds > archiveOptions.MaxSecondsInterval ||
                intervalVersiion > archiveOptions.MaxVersionInterval;
        }
    }
}
