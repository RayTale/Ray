using Ray.Core.EventSourcing;
using Ray.IGrains;
using System;

namespace Ray.Grain.ToReadActors
{
    public abstract class PSQLToReadGrain<K, S> : MongoES.MongoToReadGrain<K, MessageInfo>
        where S : class, IState<K>, new()
    {
        protected override bool ExecureExceptionFilter(Exception exception)
        {
            if (exception.InnerException is Npgsql.PostgresException e && e.SqlState == "23505")
            {
                return true;
            }
            return false;
        }
    }
}
