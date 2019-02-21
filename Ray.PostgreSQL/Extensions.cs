using System;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Storage.PostgreSQL
{
    public static class Extensions
    {
        public static void AddPostgreSQLStorage(this IServiceCollection serviceCollection, Action<SqlConfig> configAction)
        {
            serviceCollection.Configure<SqlConfig>(config => configAction(config));
            serviceCollection.AddSingleton<StorageFactory>();
        }
    }
}
