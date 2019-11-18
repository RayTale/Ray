using Microsoft.Extensions.DependencyModel;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;

namespace Ray.Core.Utils
{
    public class AssemblyHelper
    {
        public static IList<Assembly> GetAssemblies(ILogger logger)
        {
            var libs = DependencyContext.Default.CompileLibraries.Where(lib => !lib.Serviceable);
            return libs.Select(lib =>
             {
                 try
                 {
                     return AssemblyLoadContext.Default.LoadFromAssemblyName(new AssemblyName(lib.Name));
                 }
                 catch (Exception ex)
                 {
                     logger.LogWarning(ex, ex.Message);
                     return default;
                 }
             }).Where(assembly => assembly != default).ToList();
        }
    }
}
