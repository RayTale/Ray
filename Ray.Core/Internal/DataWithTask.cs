using System.Threading.Tasks;

namespace Ray.Core.Internal
{
    public class DataWithTask<T, R>
    {
        public DataWithTask(T message)
        {
            Value = message;
            TaskSource = new TaskCompletionSource<R>();
        }
        public TaskCompletionSource<R> TaskSource { get; private set; }
        public T Value { get; set; }
    }
}
