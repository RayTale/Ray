using System.Threading;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace Ray.Core.Services
{
    [Reentrant]
    public class LockGrain : Grain, ILock
    {
        private int locked = 0;
        private TaskCompletionSource<bool> taskSource;

        public async Task<bool> Lock(int millisecondsDelay = 0)
        {
            if (this.locked == 0)
            {
                this.locked = 1;
                return true;
            }
            else
            {
                this.taskSource = new TaskCompletionSource<bool>();
                if (millisecondsDelay != 0)
                {
                    using var tc = new CancellationTokenSource(millisecondsDelay);
                    tc.Token.Register(() =>
                    {
                        this.taskSource.TrySetCanceled();
                    });
                }

                return await this.taskSource.Task;
            }
        }

        public Task Unlock()
        {
            this.locked = 0;
            this.taskSource.TrySetResult(true);
            return Task.CompletedTask;
        }
    }
}
