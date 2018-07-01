using System;
using System.Collections.Generic;
using System.Linq;

namespace Ray.Core.Message
{
    public static class MessageTypeMapper
    {
        static MessageTypeMapper()
        {
            var assemblyList = AppDomain.CurrentDomain.GetAssemblies().Where(a => !a.IsDynamic);
            var eventType = typeof(IMessage);
            foreach (var assembly in assemblyList)
            {
                var allType = assembly.GetExportedTypes().Where(t => eventType.IsAssignableFrom(t) && t.IsClass && t.GetConstructors().Any(c => c.GetParameters().Length == 0));
                foreach (var type in allType)
                {
                    if (Activator.CreateInstance(type) is IMessage msg)
                    {
                        if (!string.IsNullOrEmpty(msg.TypeCode))
                            EventTypeDict.Add(msg.TypeCode, type);
                        else
                            EventTypeDict.Add(type.FullName, type);
                    }
                }
            }
        }
        public static Dictionary<string, Type> EventTypeDict { get; } = new Dictionary<string, Type>();
    }
}
