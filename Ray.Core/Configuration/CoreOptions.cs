namespace Ray.Core.Configuration
{
    public class CoreOptions
    {
        /// <summary>
        /// RayGrain保存快照的事件Version间隔
        /// </summary>
        public int SnapshotVersionInterval { get; set; } = 500;
        /// <summary>
        /// RayGrain失活的时候保存快照的最小事件Version间隔
        /// </summary>
        public int MinSnapshotVersionInterval { get; set; } = 1;
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
        public int EventAsyncProcessSecondsTimeout { get; set; } = 30;
        /// <summary>
        /// 当Grain Over时是否归档事件
        /// 归档事件操作受ArchiveOption配置影响
        /// </summary>
        public bool ArchiveEventOnOver { get; set; } = true;
        /// <summary>
        /// 优先异步事件流
        /// </summary>
        public bool PriorityAsyncEventBus { get; set; } = true;
        /// <summary>
        /// 事务超时时间(默认为30s)
        /// </summary>
        public int TransactionMillisecondsTimeout { get; set; } = 30 * 1000;
        /// <summary>
        /// 事务完成时候是否清理事务事件(事务事件只用来处理事务，不会影响状态)
        /// true:减少事件量，但是会带来额外的性能开销
        /// 默认为(false)
        /// </summary>
        public bool ClearTransactionEvents { get; set; } = true;
    }
}
