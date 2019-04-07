using System;
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
            return GetFactory("Npgsql.NpgsqlFactory, Npgsql, Culture=neutral, PublicKeyToken=5d8b90d52f46fda7");
        }
        protected static DbProviderFactory GetFactory(string assemblyQualifiedName)
        {
            return (DbProviderFactory)Type.GetType(assemblyQualifiedName).GetField("Instance").GetValue(null);
        }
    }
}
