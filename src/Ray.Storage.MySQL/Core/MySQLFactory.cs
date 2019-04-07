using System;
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
            return GetFactory("MySql.Data.MySqlClient.MySqlClientFactory, MySql.Data, Culture=neutral, PublicKeyToken=c5687fc88969c44d");
        }
        protected static DbProviderFactory GetFactory(string assemblyQualifiedName)
        {
            return (DbProviderFactory)Type.GetType(assemblyQualifiedName).GetField("Instance").GetValue(null);
        }
    }
}
