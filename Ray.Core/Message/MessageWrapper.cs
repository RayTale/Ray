using Orleans.Concurrency;

namespace Ray.Core
{

    public interface MessageWrapper
    {
        string TypeCode { get; set; }
        byte[] BinaryBytes { get; set; }
    }
}
