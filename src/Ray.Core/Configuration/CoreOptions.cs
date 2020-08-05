namespace Ray.Core.Configuration
{
    public class CoreOptions
    {
        /// <summary>
        /// Event Version interval of RayGrain saving snapshot
        /// </summary>
        public int SnapshotVersionInterval { get; set; } = 500;

        /// <summary>
        /// The minimum event Version interval for saving snapshots when RayGrain is deactivated
        /// </summary>
        public int MinSnapshotVersionInterval { get; set; } = 1;

        /// <summary>
        /// Event Version interval for ObserverGrain to save snapshots
        /// </summary>
        public int ObserverSnapshotVersionInterval { get; set; } = 20;

        /// <summary>
        /// The amount of data read each time when reading events in batches
        /// </summary>
        public int NumberOfEventsPerRead { get; set; } = 2000;

        /// <summary>
        /// Whether to archive events when Grain Over
        /// Archive event operation is affected by ArchiveOption configuration
        /// </summary>
        public bool ArchiveEventOnOver { get; set; } = true;

        /// <summary>
        /// Priority asynchronous event stream
        /// </summary>
        public bool PriorityAsyncEventBus { get; set; } = true;

        /// <summary>
        /// Transaction timeout time, in ms (default is 30s)
        /// </summary>
        public int TransactionTimeout { get; set; } = 30 * 1000;

        /// <summary>
        /// Synchronize all observers when activated
        /// </summary>
        public bool SyncAllObserversOnActivate { get; set; } = false;
    }
}