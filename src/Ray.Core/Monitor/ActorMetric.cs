using System;
using System.Collections.Generic;
using System.Text;

namespace Ray.Core.Monitor
{
   public class ActorMetric
    {
        public string ActorName { get; set; }
        public int AvgLives { get; set; }
        public int MaxLives { get; set; }
        public int AvgEventsPerActor { get; set; }
        public int MaxEventsPerActor { get; set; }
        public int MinEventsPerActor { get; set; }
        /// <summary>
        /// 统计时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
