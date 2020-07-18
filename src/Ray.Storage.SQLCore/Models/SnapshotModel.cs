﻿namespace Ray.Storage.SQLCore
{
    public class SnapshotModel<TPrimaryKey>
    {
        public TPrimaryKey StateId { get; set; }

        public long Version { get; set; }

        public long StartTimestamp { get; set; }

        public long LatestMinEventTimestamp { get; set; }

        public bool IsLatest { get; set; }

        public bool IsOver { get; set; }

        public string Data { get; set; }
    }
}
