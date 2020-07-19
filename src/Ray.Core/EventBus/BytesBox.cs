namespace Ray.Core.EventBus
{
    public class BytesBox
    {
        public BytesBox(byte[] value, object origin)
        {
            this.Value = value;
            this.Origin = origin;
        }

        public byte[] Value { get; }

        public bool Success { get; set; }

        public object Origin { get; set; }
    }
}
