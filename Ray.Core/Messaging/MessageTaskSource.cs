using System.Threading.Tasks;

namespace Ray.Core.Messaging
{
    public class MessageTaskSource<T, R>
    {
        public MessageTaskSource(T message)
        {
            Value = message;
            TaskSource = new TaskCompletionSource<R>();
        }
        public TaskCompletionSource<R> TaskSource { get; private set; }
        public T Value { get; set; }
    }
}
