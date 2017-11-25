using System;
using System.Collections.Generic;
using System.Linq;

namespace Ray.Core.Message
{
    public static class MessageTypeMapping
    {
        static MessageTypeMapping()
        {
            var assemblyList = AppDomain.CurrentDomain.GetAssemblies().Where(a => !a.IsDynamic);
            var eventType = typeof(IMessage);
            foreach (var assembly in assemblyList)
            {
                var allType = assembly.GetExportedTypes().Where(t => eventType.IsAssignableFrom(t));
                foreach (var type in allType)
                {
                    EventTypeDict.Add(type.FullName, type);
                }
            }
        }
        static Dictionary<string, Type> EventTypeDict = new Dictionary<string, Type>();
        /// <summary>
        /// 根据typecode获取Type对象
        /// </summary>
        /// <param name="typeCode">Type对象的唯一标识</param>
        /// <returns></returns>
        public static Type GetType(string typeCode)
        {
            Type type = null;
            EventTypeDict.TryGetValue(typeCode, out type);
            return type;
        }
    }
}
