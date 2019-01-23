namespace Ray.Core.State
{
    public class BriefArchive
    {
        public string Id { get; set; }
        public long StartVersion { get; set; }
        public long EndVersion { get; set; }
        public long StartTimestamp { get; set; }
        public long EndTimestamp { get; set; }
        public int Index { get; set; }
        /// <summary>
        /// 事件是否已经清理
        /// </summary>
        public bool EventIsCleared { get; set; }

    }
}
