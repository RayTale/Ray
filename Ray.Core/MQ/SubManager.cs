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
        protected List<SubAttribute> Parse(Assembly[] assemblys)
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
                                collection.AddSingleton(type);
                                result.Add(value);
                            }
                        }
                    }
                }
            }
            provider = collection.BuildServiceProvider();
            return result;
        }
        ServiceCollection collection = new ServiceCollection();
        IServiceProvider provider = default;
        public Task Start(Assembly[] assemblys, string[] types = null, string node = null, List<string> nodeList = null)
        {
            var attrlist = Parse(assemblys);
            if (types != null) attrlist = attrlist.Where(a => types.Contains(a.Type)).ToList();
            return Start(attrlist, provider, node, nodeList);
        }

        protected abstract Task Start(List<SubAttribute> attributes, IServiceProvider provider, string node = null, List<string> nodeList = null);
    }
}
