namespace Ray.Core.Event
{
    public class TransactionTransport<PrimaryKey>
    {
        public TransactionTransport(IFullyEvent<PrimaryKey> fullyEvent, string uniqueId, string hashKey, bool syncEventStream)
        {
            FullyEvent = fullyEvent;
            UniqueId = uniqueId;
            HashKey = hashKey;
            SyncEventStream = syncEventStream;
        }
        public IFullyEvent<PrimaryKey> FullyEvent { get; set; }
        public BytesTransport BytesTransport { get; set; }
        public string UniqueId { get; set; }
        public string HashKey { get; set; }
        public bool SyncEventStream { get; set; }
    }
}
