namespace Ray.Core
{
    public interface IMessageWrapper
    {
        string TypeCode { get; set; }
        byte[] BinaryBytes { get; set; }
    }
}
