namespace Ray.Core.State
{
    public  class StateArchiveOptions
    {
        public int IntervalMilliseconds { get; set; }
        public long IntervalVersion { get; set; }
        public int MaxMilliSeconds { get; set; }
        public long MaxIntervaleVersion { get; set; }
    }
}