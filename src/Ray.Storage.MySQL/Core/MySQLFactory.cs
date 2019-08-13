using System.Data.Common;

namespace Ray.Storage.MySQL
{
    public class MySQLFactory
    {
        static readonly DbProviderFactory dbFactory;
        static MySQLFactory()
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
            return MySql.Data.MySqlClient.MySqlClientFactory.Instance;
        }
    }
}
