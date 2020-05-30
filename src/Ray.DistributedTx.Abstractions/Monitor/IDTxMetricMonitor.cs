namespace Ray.DistributedTx.Abstractions
{
    public interface IDTxMetricMonitor
    {
        void Report(DTxMetricElement element);
    }
}
