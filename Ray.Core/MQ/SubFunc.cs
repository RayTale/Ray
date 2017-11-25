using System;
using System.Threading.Tasks;

namespace Ray.Core.MQ
{
    public class SubFunc<T>
    {
        public Type MessageType { get; set; }
        public Func<T, object, Task> Func { get; set; }
    }
}
