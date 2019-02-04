namespace Ray.Core.Configuration
{
    public class CoreOptions<GrainState>
    {
        /// <summary>
        /// Grain事务超时的时间(s)
        /// </summary>
        public int TransactionTimeoutSeconds { get; set; } = 60;
        /// <summary>
        /// RayGrain保存快照的事件Version间隔
        /// </summary>
        public int SnapshotIntervalVersion { get; set; } = 500;
        /// <summary>
        /// RayGrain失活的时候保存快照的最小事件Version间隔
        /// </summary>
        public int MinSnapshotIntervalVersion { get; set; } = 1;
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
        /// <summary>
        /// 当Grain Over时是否清理事件
        /// </summary>
        public bool ClearEventWhenOver { get; set; } = true;
        /// <summary>
        /// 优先异步事件流
        /// </summary>
        public bool PriorityAsyncEventBus { get; set; } = true;
    }
}
