using System;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.Event
{
    public static class EventHandlerExtensions
    {
        public static void AutoAddEventHandler(this IServiceCollection serviceCollection)
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (var type in assembly.GetTypes())
                {
                    var handlerType = type.GetInterfaces().SingleOrDefault(t => !string.IsNullOrEmpty(t.FullName) && t.FullName.StartsWith("Ray.Core.Event.IEventHandler"));
                    if (handlerType != default && !type.IsAbstract)
                        serviceCollection.AddSingleton(handlerType, type);
                }
            }
        }
    }
}
