namespace Ray.Core.Channels
{
    public class ChannelOptions
    {
        /// <summary>
        /// 批量数据处理每次处理的最大数据量
        /// </summary>
        public int MaxSizeOfBatch { get; set; } = 5000;
    }
}
