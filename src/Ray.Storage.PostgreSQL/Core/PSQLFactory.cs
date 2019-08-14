using System.Data.Common;

namespace Ray.Storage.PostgreSQL
{
    public class PSQLFactory
    {
        static readonly DbProviderFactory dbFactory;
        static PSQLFactory()
        {
            dbFactory = GetPostgreSqlFactory();
        }
        public static DbConnection CreateConnection(string conn)
        {
            var connection = dbFactory.CreateConnection();
            connection.ConnectionString = conn;
            return connection;
        }
        protected static DbProviderFactory GetPostgreSqlFactory()
        {
            return Npgsql.NpgsqlFactory.Instance;
        }
    }
}
