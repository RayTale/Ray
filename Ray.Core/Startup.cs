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
        static List<FuncWrapper> methods = new List<FuncWrapper>();
        public static void Register(Func<IServiceProvider, Task> method, int sortIndex = 0)
        {
            methods.Add(new FuncWrapper(sortIndex, method));
        }
        internal static Task StartRay(IServiceProvider serviceProvider)
        {
            methods.Add(new FuncWrapper(int.MaxValue, provider => provider.GetService<IConsumerManager>().Start()));
            methods = methods.OrderBy(func => func.SortIndex).ToList();
            return Task.WhenAll(methods.Select(value => value.Func(serviceProvider)));
        }
        private class FuncWrapper
        {
            public FuncWrapper(int sortIndex, Func<IServiceProvider, Task> func)
            {
                SortIndex = sortIndex;
                Func = func;
            }
            public int SortIndex { get; set; }
            public Func<IServiceProvider, Task> Func { get; set; }
        }
    }
}
