using System.Threading;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace Ray.Core.Services
{
    [Reentrant]
    public class LockGrain : Grain, ILock
    {
        int locked = 0;
        TaskCompletionSource<bool> taskSource;
        public async Task<bool> Lock(int millisecondsDelay = 0)
        {
            if (locked == 0)
            {
                locked = 1;
                return true;
            }
            else
            {
                taskSource = new TaskCompletionSource<bool>();
                if (millisecondsDelay != 0)
                {
                    using var tc = new CancellationTokenSource(millisecondsDelay);
                    tc.Token.Register(() =>
                    {
                        taskSource.TrySetCanceled();
                    });
                }
                return await taskSource.Task;
            }
        }

        public Task Unlock()
        {
            locked = 0;
            taskSource.TrySetResult(true);
            return Task.CompletedTask;
        }
    }
}
