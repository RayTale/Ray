namespace Ray.Core.Serialization
{
    public interface IBytesWrapper
    {
        string TypeName { get; set; }
        byte[] Bytes { get; set; }
    }
}
