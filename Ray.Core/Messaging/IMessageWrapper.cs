namespace Ray.Core
{

    public interface IMessageWrapper
    {
        string TypeName { get; set; }
        byte[] Bytes { get; set; }
    }
}
