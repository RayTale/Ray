using System;
using System.Data.Common;

namespace Ray.PostgreSQL
{
    public class SqlFactory
    {
        static DbProviderFactory dbFactory;
        static SqlFactory()
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
            return GetFactory("Npgsql.NpgsqlFactory, Npgsql, Culture=neutral, PublicKeyToken=5d8b90d52f46fda7");
        }
        protected static DbProviderFactory GetFactory(string assemblyQualifiedName)
        {
            return (DbProviderFactory)Type.GetType(assemblyQualifiedName).GetField("Instance").GetValue(null);
        }
    }
}
