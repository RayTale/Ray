using System;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.Event
{
    public static class AutoAddEventHandler
    {
        public static void AddEventHandler(this IServiceCollection serviceCollection)
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (var type in assembly.GetTypes())
                {
                    var handlerType = type.GetInterfaces().SingleOrDefault(t => t.Name.StartsWith("IEventHandler"));
                    if (handlerType != default)
                        serviceCollection.AddSingleton(handlerType, type);
                }
            }
        }
    }
}
