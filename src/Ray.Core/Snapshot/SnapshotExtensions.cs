using System;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;

namespace Ray.Core.Snapshot
{
    public static class SnapshotExtensions
    {
        public static void AutoAddSnapshotHandler(this IServiceCollection serviceCollection)
        {
            foreach (var assembly in AssembliesRuntimeHelper.GetAssemblies())
            {
                foreach (var type in assembly.GetTypes())
                {
                    var handlerType = type.GetInterfaces().SingleOrDefault(t => !string.IsNullOrEmpty(t.FullName) && t.FullName.StartsWith("Ray.Core.Snapshot.ISnapshotHandler"));
                    if (handlerType != null && !type.IsAbstract)
                        serviceCollection.AddSingleton(handlerType, type);
                }
            }
        }
    }
}
