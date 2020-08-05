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
        /// Whether the event has been cleaned up
        /// </summary>
        public bool EventIsCleared { get; set; }

        public bool IsCompletedArchive(ArchiveOptions archiveOptions, ArchiveBrief preArchive = default)
        {
            var intervalMilliseconds = (preArchive is null ? this.EndTimestamp - this.StartTimestamp : this.EndTimestamp - preArchive.EndTimestamp) / 1000;
            var intervalVersion = this.EndVersion - this.StartVersion;
            return (intervalMilliseconds > archiveOptions.SecondsInterval && intervalVersion > archiveOptions.VersionInterval) ||
                intervalMilliseconds > archiveOptions.MaxSecondsInterval ||
                intervalVersion > archiveOptions.MaxVersionInterval;
        }
    }
}
