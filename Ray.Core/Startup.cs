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
        static readonly List<FuncWrapper> methods = new List<FuncWrapper>();
        public static void Register(Func<IServiceProvider, Task> method, int sortIndex = 0)
        {
            methods.Add(new FuncWrapper(sortIndex, method));
        }
        internal static Task StartRay(IServiceProvider serviceProvider)
        {
            methods.Add(new FuncWrapper(int.MaxValue, provider => provider.GetService<IConsumerManager>().Start()));
            methods.Sort(new FuncComparer());
            return Task.WhenAll(methods.Select(value => value.Func(serviceProvider)));
        }
        private class FuncComparer : IComparer<FuncWrapper>
        {
            public int Compare(FuncWrapper x, FuncWrapper y)
            {
                if (x.SortIndex > y.SortIndex)
                    return 0;
                else
                    return -1;
            }
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
