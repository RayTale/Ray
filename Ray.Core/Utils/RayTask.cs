using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.Core.Utils
{
    public static class RayTask
    {
        public static Task<T> Execute<T>(Func<Task<T>> func, int millisecondsDelay = 5000)
        {
            var ts = new TaskCompletionSource<T>();
            var token = new CancellationTokenSource(millisecondsDelay);
            Task.Run(func, token.Token).ContinueWith((t) =>
            {
                if (t.Exception != null)
                {
                    ts.TrySetException(t.Exception);
                }
                else if (t.IsCanceled)
                {
                    ts.TrySetCanceled();
                }
                else
                {
                    ts.TrySetResult(t.Result);
                }
            }).ConfigureAwait(false);
            return ts.Task;
        }
        public static Task Execute(Func<Task> func, int millisecondsDelay = 5000)
        {
            var ts = new TaskCompletionSource<bool>();
            var token = new CancellationTokenSource(millisecondsDelay);
            Task.Run(func, token.Token).ContinueWith((t) =>
            {
                if (t.Exception != null)
                {
                    ts.TrySetException(t.Exception);
                }
                else if (t.IsCanceled)
                {
                    ts.TrySetCanceled();
                }
                else
                {
                    ts.TrySetResult(true);
                }
            }).ConfigureAwait(false);
            return ts.Task;
        }
    }
}
