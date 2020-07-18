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
        public EventUID(long timestamp, string fromEvent, string fromActor, string fromActorId, long fromVersion)
        {
            UID = $"{fromActorId}_{fromVersion}";
            Timestamp = timestamp;
            FromEvent = fromEvent;
            FromActor = fromActor;
            FromActorId = fromActorId;
            FromVersion = fromVersion;
        }
        public EventUID(string uid, long timestamp, string fromEvent, string fromActor, string fromActorId, long fromVersion)
        {
            if (string.IsNullOrWhiteSpace(uid))
                throw new ArgumentNullException(nameof(uid));

            UID = uid;
            Timestamp = timestamp;
            FromEvent = fromEvent;
            FromActor = fromActor;
            FromActorId = fromActorId;
            FromVersion = fromVersion;
        }
        /// <summary>
        /// 唯一Id
        /// </summary>
        public string UID { get; }
        /// <summary>
        /// 时间戳
        /// </summary>
        public long Timestamp { get; }
        /// <summary>
        /// 前一个事件的类型名称
        /// </summary>
        public string FromEvent { get; }
        /// <summary>
        /// 来源的Actor
        /// </summary>
        public string FromActor { get; }
        /// <summary>
        /// 前一个事件的StateId
        /// </summary>
        public string FromActorId { get; }
        /// <summary>
        /// 前一个事件的版本
        /// </summary>
        public long FromVersion { get; }
    }
}
