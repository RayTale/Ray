namespace Ray.Core.Event
{
    public class TransactionTransport<PrimaryKey>
    {
        public TransactionTransport(IFullyEvent<PrimaryKey> fullyEvent, string uniqueId = null, string hashKey = null)
        {
            FullyEvent = fullyEvent;
            UniqueId = uniqueId;
            HashKey = hashKey;
        }
        public IFullyEvent<PrimaryKey> FullyEvent { get; set; }
        public BytesTransport BytesTransport { get; set; }
        public string UniqueId { get; set; }
        public string HashKey { get; set; }
    }
}
