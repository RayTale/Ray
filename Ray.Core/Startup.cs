using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

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
    }
}
