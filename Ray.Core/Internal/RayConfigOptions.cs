namespace Ray.Core.Internal
{
    public class RayConfigOptions
    {
        /// <summary>
        /// Grain事务超时的时间(s)
        /// </summary>
        public int TransactionTimeoutSeconds { get; set; } = 60;
        /// <summary>
        /// RayGrain保存快照的事件Version间隔
        /// </summary>
        public int SnapshotVersionInterval { get; set; } = 500;
        /// <summary>
        /// RayGrain失活的时候保存快照的最小事件Version间隔
        /// </summary>
        public int SnapshotMinVersionInterval { get; set; } = 1;
        /// <summary>
        /// FollowGrain保存快照的事件Version间隔
        /// </summary>
        public int FollowSnapshotVersionInterval { get; set; } = 20;
        /// <summary>
        /// FollowGrain失活的时候保存快照的最小事件Version间隔
        /// </summary>
        public int FollowSnapshotMinVersionInterval { get; set; } = 1;
        /// <summary>
        /// 分批次批量读取事件的时候每次读取的数据量
        /// </summary>
        public int NumberOfEventsPerRead { get; set; } = 2000;
        /// <summary>
        /// 事件异步处理的超时时间
        /// </summary>
        public int EventAsyncProcessTimeoutSeconds { get; set; } = 30;
    }
}
