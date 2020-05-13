using System;
using Orleans.Concurrency;

namespace Ray.Core.Event
{
    [Immutable]
    public class EventUID
    {
        public EventUID()
        {
        }
        public EventUID(string uid, long timestamp)
        {
            if (string.IsNullOrWhiteSpace(uid))
                throw new ArgumentNullException(nameof(uid));

            UID = uid;
            Timestamp = timestamp;
        }
        public EventUID(long timestamp, string fromEvent, string fromStateId, long fromVersion)
        {
            UID = $"{fromStateId}_{fromVersion}";
            Timestamp = timestamp;
            FromEvent = fromEvent;
            FromStateId = fromStateId;
            FromVersion = fromVersion;
        }
        public EventUID(string uid, long timestamp, string fromEvent, string fromStateId, long fromVersion)
        {
            if (string.IsNullOrWhiteSpace(uid))
                throw new ArgumentNullException(nameof(uid));

            UID = uid;
            Timestamp = timestamp;
            FromEvent = fromEvent;
            FromStateId = fromStateId;
            FromVersion = fromVersion;
        }
        /// <summary>
        /// 唯一Id
        /// </summary>
        public string UID { get; set; }
        /// <summary>
        /// 时间戳
        /// </summary>
        public long Timestamp { get; set; }
        /// <summary>
        /// 前一个事件的类型名称
        /// </summary>
        public string FromEvent { get; set; }
        /// <summary>
        /// 前一个事件的StateId
        /// </summary>
        public string FromStateId { get; set; }
        /// <summary>
        /// 前一个事件的版本
        /// </summary>
        public long FromVersion { get; set; }
    }
}
