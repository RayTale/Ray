namespace Ray.DistributedTransaction.Configuration
{
    public class DistributedTxOptions
    {
        /// <summary>
        /// 事务完成时候是否清理事务事件(事务事件只用来处理事务，不会影响状态)
        /// true:减少事件量，但是会带来额外的性能开销
        /// 默认为(false)
        /// </summary>
        public bool RetainTxEvents { get; set; } = false;
    }
}
