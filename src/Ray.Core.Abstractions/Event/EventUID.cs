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
            {
                throw new ArgumentNullException(nameof(uid));
            }

            this.UID = uid;
            this.Timestamp = timestamp;
        }

        public EventUID(long timestamp, string fromEvent, string fromActor, string fromActorId, long fromVersion)
        {
            this.UID = $"{fromActorId}_{fromVersion}";
            this.Timestamp = timestamp;
            this.FromEvent = fromEvent;
            this.FromActor = fromActor;
            this.FromActorId = fromActorId;
            this.FromVersion = fromVersion;
        }

        public EventUID(string uid, long timestamp, string fromEvent, string fromActor, string fromActorId, long fromVersion)
        {
            if (string.IsNullOrWhiteSpace(uid))
            {
                throw new ArgumentNullException(nameof(uid));
            }

            this.UID = uid;
            this.Timestamp = timestamp;
            this.FromEvent = fromEvent;
            this.FromActor = fromActor;
            this.FromActorId = fromActorId;
            this.FromVersion = fromVersion;
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
