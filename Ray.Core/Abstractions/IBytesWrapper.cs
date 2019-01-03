namespace Ray.Core.Abstractions
{
    public interface IBytesWrapper
    {
        string TypeName { get; set; }
        byte[] Bytes { get; set; }
    }
}
