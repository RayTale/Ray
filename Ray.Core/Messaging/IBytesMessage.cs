namespace Ray.Core.Messaging
{

    public interface IBytesMessage
    {
        string TypeName { get; set; }
        byte[] Bytes { get; set; }
    }
}
