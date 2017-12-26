using Ray.Core.MQ;
using Ray.RabbitMQ;
using System.Threading.Tasks;
using Ray.IGrains;

namespace Ray.Handler
{
    [RabbitSub("Read", "Account", "account")]
    public abstract class PSQLToReadHandler<K> : PartSubHandler<K, MessageInfo>
    {
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
