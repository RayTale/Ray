using System.Data.Common;

namespace Ray.Storage.SQLServer
{
    public class SQLServerFactory
    {
        static readonly DbProviderFactory dbFactory;
        static SQLServerFactory()
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
            return Microsoft.Data.SqlClient.SqlClientFactory.Instance;
        }
    }
}
