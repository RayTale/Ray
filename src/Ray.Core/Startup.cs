using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Ray.Core
{
    public static class Startup
    {
        private static List<StartupTask> tasks = new List<StartupTask>();

        public static void Register(Func<IServiceProvider, Task> method, int sortIndex = 0)
        {
            tasks.Add(new StartupTask(sortIndex, method));
        }

        internal static Task StartRay(IServiceProvider serviceProvider)
        {
            tasks = tasks.OrderBy(func => func.SortIndex).ToList();
            return Task.WhenAll(tasks.Select(value => value.Func(serviceProvider)));
        }

        private class StartupTask
        {
            public StartupTask(int sortIndex, Func<IServiceProvider, Task> func)
            {
                this.SortIndex = sortIndex;
                this.Func = func;
            }

            public int SortIndex { get; set; }

            public Func<IServiceProvider, Task> Func { get; set; }
        }
    }
}
