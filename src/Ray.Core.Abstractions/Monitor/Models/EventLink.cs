namespace Ray.Core.Abstractions.Monitor
{
    public class EventLink
    {
        /// <summary>
        /// Actor的类型
        /// </summary>
        public string Actor { get; set; }
        /// <summary>
        /// 当前事件类型名称
        /// </summary>
        public string Event { get; set; }
        /// <summary>
        /// 上级
        /// </summary>
        public EventLink Parent { get; set; }
    }
}
