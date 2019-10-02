namespace Ray.DistributedTx.Configuration
{
    public class DistributedTxOptions
    {
        /// <summary>
        /// 事务完成时候是否保留事务事件(事务事件只用来处理事务，不会影响状态)
        /// true:保留事务事件，提高吞吐，单位额外增加事件量
        /// 默认为(false)
        /// </summary>
        public bool RetainTxEvents { get; set; } = false;
    }
}
