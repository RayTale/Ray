using System;
using System.Collections.Concurrent;

namespace Ray.Core.Messaging
{
    public static class TypeContainer
    {
        private static readonly ConcurrentDictionary<string, Type> typeDict  = new ConcurrentDictionary<string, Type>();
        public static bool TryGetValue(string typeName, out Type value)
        {
            value = typeDict.GetOrAdd(typeName, key =>
            {
                return Type.GetType(typeName, false);
            });
            return value == default;
        }
    }
}
