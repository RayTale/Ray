using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.EventBus;

namespace Ray.Core
{
    public static class Startup
    {
        static readonly List<Func<IServiceProvider, Task>> methods = new List<Func<IServiceProvider, Task>>();
        public static void Register(Func<IServiceProvider, Task> method)
        {
            methods.Add(method);
        }
        internal static Task StartRay(IServiceProvider serviceProvider)
        {
            methods.Add(provider => provider.GetService<IConsumerManager>().Start());
            return Task.WhenAll(methods.Select(m => m(serviceProvider)));
        }
    }
}
