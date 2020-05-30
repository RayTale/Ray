namespace Ray.Metrics
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
        /// 上级Actor
        /// </summary>
        public string ParentActor { get; set; }
        /// <summary>
        /// 上级事件
        /// </summary>
        public string ParentEvent { get; set; }
    }
}
