namespace Ray.Storage.SQLCore
{
    /// <summary>
    /// Sub-table information
    /// </summary>
    public class EventSubTable
    {
        /// <summary>
        /// Table Name
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Sub-table name
        /// </summary>
        public string SubTable { get; set; }

        /// <summary>
        /// Sub-table order
        /// </summary>
        public int Index { get; set; }

        /// <summary>
        /// The start time of the sub-table
        /// </summary>
        public long StartTime { get; set; }

        /// <summary>
        /// End time of sub-table
        /// </summary>
        public long EndTime { get; set; }
    }
}