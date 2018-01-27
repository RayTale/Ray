using Ray.Core.MQ;
using Ray.RabbitMQ;
using System.Threading.Tasks;
using Ray.IGrains;
using System;

namespace Ray.Handler
{
    public abstract class PSQLToReadHandler<K> : PartSubHandler<K, MessageInfo>
    {
        public PSQLToReadHandler(IServiceProvider svProvider) : base(svProvider)
        { }
        public override Task Notice(byte[] data)
        {
            return base.Notice(data).ContinueWith(t =>
            {
                if (t.Exception != null)
                {
                    if (!(t.Exception.InnerException is Npgsql.PostgresException e && e.SqlState == "23505"))
                    {
                        throw t.Exception;
                    }
                }
            });
        }
    }
}
