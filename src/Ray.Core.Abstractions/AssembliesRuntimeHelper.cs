using Microsoft.Extensions.DependencyModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Ray.Core.Abstractions
{
    public class AssembliesRuntimeHelper
    {
        public static IList<Assembly> GetAssemblies()
        {
            var list = new List<Assembly>();
            var deps = DependencyContext.Default;
            var libs = deps.CompileLibraries.Where(lib => !lib.Serviceable && lib.Type != "package");//排除所有的系统程序集、Nuget下载包
            foreach (var lib in libs)
            {
                try
                {
                    var assembly = System.Runtime.Loader.AssemblyLoadContext.Default.LoadFromAssemblyName(new AssemblyName(lib.Name));
                    list.Add(assembly);
                }
                catch (Exception)
                {
                    // ignored
                }
            }
            return list;
        }
    }
}
