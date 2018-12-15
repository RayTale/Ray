using System.Threading.Tasks;

namespace Ray.Core.Messaging
{
    public class MessageTaskWrapper<T, R>
    {
        public MessageTaskWrapper(T message)
        {
            Value = message;
            TaskSource = new TaskCompletionSource<R>();
        }
        public TaskCompletionSource<R> TaskSource { get; private set; }
        public T Value { get; set; }
    }
}
