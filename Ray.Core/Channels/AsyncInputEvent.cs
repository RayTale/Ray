using System.Threading.Tasks;

namespace Ray.Core.Channels
{
    public class AsyncInputEvent<Input, Output>
    {
        public AsyncInputEvent(Input data)
        {
            Value = data;
            TaskSource = new TaskCompletionSource<Output>();
        }
        public TaskCompletionSource<Output> TaskSource { get; private set; }
        public Input Value { get; set; }
    }
}
