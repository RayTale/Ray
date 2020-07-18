using System.Data.Common;

namespace Ray.Storage.MySQL
{
    public class MySQLFactory
    {
        private static readonly DbProviderFactory dbFactory;

        static MySQLFactory()
        {
            dbFactory = GetDbProviderFactory();
        }

        public static DbConnection CreateConnection(string conn)
        {
            var connection = dbFactory.CreateConnection();
            connection.ConnectionString = conn;
            return connection;
        }

        protected static DbProviderFactory GetDbProviderFactory()
        {
            return MySql.Data.MySqlClient.MySqlClientFactory.Instance;
        }
    }
}
