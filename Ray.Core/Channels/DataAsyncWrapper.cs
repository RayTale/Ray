using System.Threading.Tasks;

namespace Ray.Core.Channels
{
    public class DataAsyncWrapper<Input, Output>
    {
        public DataAsyncWrapper(Input data)
        {
            Value = data;
            TaskSource = new TaskCompletionSource<Output>();
        }
        public TaskCompletionSource<Output> TaskSource { get; private set; }
        public Input Value { get; set; }
    }
}
