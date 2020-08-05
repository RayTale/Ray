namespace Ray.Core.Configuration
{
    /// <summary>
    /// Archive configuration
    /// </summary>
    public class ArchiveOptions
    {
        /// <summary>
        /// Whether to open archive
        /// </summary>
        public bool On { get; set; } = true;

        /// <summary>
        /// The number of seconds that the archive must meet
        /// </summary>
        public long SecondsInterval { get; set; } = 24 * 60 * 60 * 7;

        /// <summary>
        /// The interval version number that the archive must meet
        /// </summary>
        public int VersionInterval { get; set; } = 1000;

        /// <summary>
        /// The maximum number of seconds between archiving, as long as the interval is greater than this value, you can archive (default 30 days)
        /// </summary>
        public long MaxSecondsInterval { get; set; } = 24 * 60 * 60 * 30;

        /// <summary>
        /// The maximum version number of the archive, as long as the interval is greater than this value, it can be archived
        /// </summary>
        public int MaxVersionInterval { get; set; } = 10_000_000;

        /// <summary>
        /// The minimum version interval for archiving, used to judge whether to archive incompletely when Grain is deactivated
        /// </summary>
        public long MinVersionIntervalAtDeactivate { get; set; } = 1;

        /// <summary>
        /// The number of snapshot archives since the event was archived (to prevent idempotent failure caused by too fast cleaning)
        /// </summary>
        public int MaxSnapshotArchiveRecords { get; set; } = 3;

        /// <summary>
        /// Whether to enable event cleaning
        /// true: Event will be deleted when archived
        /// false: will move the archived events to the archive event library
        /// </summary>
        public EventArchiveType EventArchiveType { get; set; } = EventArchiveType.Transfer;
    }

    /// <summary>
    /// Event archive type
    /// </summary>
    public enum EventArchiveType : byte
    {
        /// <summary>
        /// Transfer to the archive table
        /// </summary>
        Transfer = 0,

        /// <summary>
        /// Delete directly
        /// </summary>
        Delete = 1
    }
}