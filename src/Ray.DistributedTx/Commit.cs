namespace Ray.DistributedTx
{
    public class Commit<Input>
    {
        public long TransactionId { get; set; }
        public TransactionStatus Status { get; set; }
        public Input Data { get; set; }
    }
}
