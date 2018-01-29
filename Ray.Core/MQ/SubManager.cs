using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Reflection;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.MQ
{
    public abstract class SubManager : ISubManager
    {
        static Type subscribeType = typeof(SubAttribute), handlerType = typeof(ISubHandler);
        static List<SubAttribute> attrlist = null;
        public static void Parse(IServiceCollection serviceCollection, params Assembly[] assemblys)
        {
            List<SubAttribute> result = new List<SubAttribute>();
            foreach (var assembly in assemblys)
            {
                var handlerTypes = assembly.GetExportedTypes().Where(t => handlerType.IsAssignableFrom(t)).ToList();
                foreach (var type in handlerTypes)
                {
                    var attributes = type.GetCustomAttributes(subscribeType, true).ToList();
                    if (attributes.Count > 0)
                    {
                        foreach (var attr in attributes)
                        {
                            if (attr is SubAttribute value)
                            {
                                value.Handler = type;
                                serviceCollection.AddSingleton(type);
                                result.Add(value);
                            }
                        }
                    }
                }
            }
            attrlist = result;
        }
        public Task Start(string[] groups = null, string node = null, List<string> nodeList = null)
        {
            if (groups != null) attrlist = attrlist.Where(a => groups.Contains(a.Group)).ToList();
            return Start(attrlist, node, nodeList);
        }

        protected abstract Task Start(List<SubAttribute> attributes, string node = null, List<string> nodeList = null);
    }
}
