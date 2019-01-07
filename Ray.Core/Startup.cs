using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Channels;
using Ray.Core.Client;
using Ray.Core.Serialization;

namespace Ray.Core
{
    public static class Startup
    {
        static readonly List<Func<IServiceProvider, Task>> methods = new List<Func<IServiceProvider, Task>>();
        public static void Register(Func<IServiceProvider,Task> method)
        {
            methods.Add(method);
        }
        public static Task Start(IServiceProvider serviceProvider)
        {
            return Task.WhenAll(methods.Select(m=>m(serviceProvider)));
        }

        public static void AddRay(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IClusterClientFactory, ClusterClientFactory>();
            serviceCollection.AddTransient(typeof(IMpscChannel<>), typeof(MpscChannel<>));
            serviceCollection.AddSingleton<IJsonSerializer, DefaultJsonSerializer>();
        }
        public static Task StartRay(this IServiceProvider serviceProvider)
        {
            return Startup.Start(serviceProvider);
        }
    }
}
