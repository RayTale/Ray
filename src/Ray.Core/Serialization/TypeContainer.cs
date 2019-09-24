using System;
using System.Collections.Concurrent;
using System.Linq;
using Ray.Core.Event;
using Ray.Core.Exceptions;

namespace Ray.Core.Serialization
{
    public static class TypeContainer
    {
        private static readonly ConcurrentDictionary<string, Type> _CodeDict = new ConcurrentDictionary<string, Type>();
        private static readonly ConcurrentDictionary<Type, string> _TypeDict = new ConcurrentDictionary<Type, string>();
        static TypeContainer()
        {
            var assemblyList = AppDomain.CurrentDomain.GetAssemblies().Where(a => !a.IsDynamic);
            var baseEventType = typeof(IEvent);
            var attributeType = typeof(TCodeAttribute);
            foreach (var assembly in assemblyList)
            {
                foreach (var type in assembly.GetTypes())
                {
                    if (baseEventType.IsAssignableFrom(type))
                    {
                        var attribute = type.GetCustomAttributes(attributeType, false).FirstOrDefault();
                        if (attribute != null && attribute is TCodeAttribute tCode)
                        {
                            if (!_CodeDict.TryAdd(tCode.Code, type))
                            {
                                throw new TypeCodeRepeatedException(tCode.Code, type.FullName);
                            }
                            _TypeDict.TryAdd(type, tCode.Code);
                        }
                        else
                        {
                            _TypeDict.TryAdd(type, type.FullName);
                        }
                        if (!_CodeDict.TryAdd(type.FullName, type))
                        {
                            throw new TypeCodeRepeatedException(type.FullName, type.FullName);
                        }
                    }
                }
            }
        }
        /// <summary>
        /// 通过TypeCode获取Type对象
        /// </summary>
        /// <param name="typeCode"></param>
        /// <returns></returns>
        public static Type GetType(string typeCode)
        {
            var value = _CodeDict.GetOrAdd(typeCode, key =>
            {
                var assemblyList = AppDomain.CurrentDomain.GetAssemblies().Where(a => !a.IsDynamic);
                foreach (var assembly in assemblyList)
                {
                    var type = assembly.GetType(typeCode, false);
                    if (type != default)
                    {
                        return type;
                    }
                }
                return Type.GetType(typeCode, false);
            });
            if (value is null)
                throw new UnknowTypeCodeException(typeCode);
            return value;
        }
        /// <summary>
        /// 获取Type对象的TypeCode字符串
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static string GetTypeCode(Type type)
        {
            if (!_TypeDict.TryGetValue(type, out var value))
                return type.FullName;
            return value;
        }
    }
}
