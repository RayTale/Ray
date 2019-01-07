using System;
using System.Collections.Concurrent;
using System.Linq;
using Ray.Core.Exceptions;

namespace Ray.Core.Serialization
{
    public static class TypeContainer
    {
        private static readonly ConcurrentDictionary<string, Type> typeDict = new ConcurrentDictionary<string, Type>();
        public static Type GetType(string typeName)
        {
            var value = typeDict.GetOrAdd(typeName, key =>
             {
                 var assemblyList = AppDomain.CurrentDomain.GetAssemblies().Where(a => !a.IsDynamic);
                 foreach (var assembly in assemblyList)
                 {
                     var type = assembly.GetType(typeName, false);
                     if (type != default)
                     {
                         return type;
                     }
                 }
                 return Type.GetType(typeName, false);
             });
            if (value == default)
                throw new UnknowTypeNameException(typeName);
            return value;
        }
    }
}
