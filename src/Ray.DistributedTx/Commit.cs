namespace Ray.DistributedTx
{
    public class Commit<Input>
    {
        public string TransactionId { get; set; }

        public TransactionStatus Status { get; set; }

        public Input Data { get; set; }

        public long Timestamp { get; set; }
    }
}
