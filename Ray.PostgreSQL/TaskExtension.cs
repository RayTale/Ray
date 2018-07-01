using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace Ray.PostgreSQL
{
    public static class TaskExtension
    {
        public static Task IdempotenceFilter(this Task task)
        {
            return task.ContinueWith(t =>
            {
                if (t.Exception != null)
                {
                    foreach (var exception in t.Exception.InnerExceptions)
                    {
                        if (!(exception is Npgsql.PostgresException e && e.SqlState == "23505"))
                        {
                            ExceptionDispatchInfo.Capture(t.Exception).Throw();
                        }
                    }
                }
            });
        }
        public static Task<T> IdempotenceFilter<T>(this Task<T> task, T defaultValueOnException = default)
        {
            return task.ContinueWith(t =>
            {
                if (t.Exception != null)
                {
                    foreach (var exception in t.Exception.InnerExceptions)
                    {
                        if (!(exception is Npgsql.PostgresException e && e.SqlState == "23505"))
                        {
                            ExceptionDispatchInfo.Capture(t.Exception).Throw();
                        }
                    }
                    return defaultValueOnException;
                }
                else
                    return t.Result;
            });
        }
    }
}
