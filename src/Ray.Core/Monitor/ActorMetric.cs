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
        public int AvgEvents { get; set; }
        public int MaxEvents{ get; set; }
        public int MinEvents { get; set; }
        /// <summary>
        /// 统计时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
