namespace Ray.IGrains.TransactionUnits.Inputs
{
    public class TransferInput
    {
        public long FromId { get; set; }
        public long ToId { get; set; }
        public decimal Amount { get; set; }
    }
}
