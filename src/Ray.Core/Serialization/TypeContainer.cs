using System;
using System.Collections.Generic;
using System.Linq;
using Ray.Core.Event;
using Ray.Core.Exceptions;

namespace Ray.Core.Serialization
{
    public static class TypeContainer
    {
        private static readonly Dictionary<string, Type> _CodeDict = new Dictionary<string, Type>();
        private static readonly Dictionary<Type, string> _TypeDict = new Dictionary<Type, string>();
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
                        if (attribute != default && attribute is TCodeAttribute tCode)
                        {
                            _CodeDict.Add(tCode.Code, type);
                            _TypeDict.Add(type, tCode.Code);
                        }
                        else
                        {
                            _TypeDict.Add(type, type.FullName);
                        }
                        _CodeDict.Add(type.FullName, type);
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
            if (!_CodeDict.TryGetValue(typeCode, out var value))
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
