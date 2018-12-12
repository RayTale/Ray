namespace Ray.Core.Internal
{
    public class RayConfigOptions
    {
        public int TransactionTimeoutSeconds { get; set; } = 60;
        public int MaxDelayOfBatchMilliseconds { get; set; } = 100;
    }
}
