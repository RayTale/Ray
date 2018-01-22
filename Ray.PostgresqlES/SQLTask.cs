using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.PostgresqlES
{
    public static class SQLTask
    {
        public static Task<T> SQLTaskExecute<T>(Func<Task<T>> func, int millisecondsDelay = 5000)
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
        public static Task SQLTaskExecute(Func<Task> func, int millisecondsDelay = 5000)
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
