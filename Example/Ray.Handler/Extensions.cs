using Microsoft.Extensions.DependencyInjection;

namespace Ray.Handler
{
    public static class Extensions
    {
        public static void AddMQHandler(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<HandlerStartup>();
            serviceCollection.AddSingleton<AccountCoreHandler>();
            serviceCollection.AddSingleton<AccountRepHandler>();
            serviceCollection.AddSingleton<AccountToDbHandler>();
        }
    }
}
