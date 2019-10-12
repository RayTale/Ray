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
        /// ObserverGrain保存快照的事件Version间隔
        /// </summary>
        public int ObserverSnapshotVersionInterval { get; set; } = 20;
        /// <summary>
        /// 分批次批量读取事件的时候每次读取的数据量
        /// </summary>
        public int NumberOfEventsPerRead { get; set; } = 2000;
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
        /// 事务超时时间,单位为ms(默认为30s)
        /// </summary>
        public int TransactionTimeout { get; set; } = 30 * 1000;
    }
}
