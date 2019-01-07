using System.Threading.Tasks;

namespace Ray.Core.Channels
{
    public class DataAsyncWrapper<T, R>
    {
        public DataAsyncWrapper(T data)
        {
            Value = data;
            TaskSource = new TaskCompletionSource<R>();
        }
        public TaskCompletionSource<R> TaskSource { get; private set; }
        public T Value { get; set; }
    }
}
