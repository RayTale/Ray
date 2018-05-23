using Ray.Core.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Ray.Core.Message
{
    public static class MessageTypeMapper
    {
        static readonly Dictionary<string, Type> typeDict = new Dictionary<string, Type>();
        static MessageTypeMapper()
        {
            var assemblyList = AppDomain.CurrentDomain.GetAssemblies().Where(a => !a.IsDynamic);
            var eventType = typeof(IMessage);
            foreach (var assembly in assemblyList)
            {
                var allType = assembly.GetExportedTypes().Where(t => eventType.IsAssignableFrom(t));
                foreach (var type in allType)
                {
                    typeDict.Add(type.FullName, type);
                }
            }
        }
        public static Type GetType(string typeCode)
        {
            if (typeDict.TryGetValue(typeCode, out var type))
                return type;
            else
            {
                throw new UnknowTypeCodeException(typeCode);
            }
        }
    }
}
