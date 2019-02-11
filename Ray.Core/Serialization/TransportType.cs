namespace Ray.Core.Serialization
{
    /// <summary>
    /// 消息序列化传输的类别
    /// </summary>
    public enum TransportType : byte
    {
        /// <summary>
        /// 通用序列化消息
        /// </summary>
        Common = 0,
        /// <summary>
        /// 事件序列化消息
        /// </summary>
        Event = 1
    }
}
