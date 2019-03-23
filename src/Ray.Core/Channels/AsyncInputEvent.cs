using System.Threading.Tasks;

namespace Ray.Core.Channels
{
    public class AsyncInputEvent<Input, Output>
    {
        public AsyncInputEvent(Input data)
        {
            Value = data;
        }
        public TaskCompletionSource<Output> TaskSource { get; } = new TaskCompletionSource<Output>();
        public Input Value { get; set; }
    }
}
