﻿using System;
using System.Collections.Concurrent;
using System.Linq;
using Microsoft.Extensions.Logging;
using Ray.Core.Abstractions;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Utils;

namespace Ray.Core.Serialization
{
    public class TypeFinder : ITypeFinder
    {
        private readonly ConcurrentDictionary<string, Type> codeDict = new ConcurrentDictionary<string, Type>();
        private readonly ConcurrentDictionary<Type, string> typeDict = new ConcurrentDictionary<Type, string>();
        private readonly ILogger<TypeFinder> logger;

        public TypeFinder(ILogger<TypeFinder> logger)
        {
            this.logger = logger;
            var baseEventType = typeof(IEvent);
            var attributeType = typeof(EventNameAttribute);
            foreach (var assembly in AssemblyHelper.GetAssemblies(this.logger))
            {
                foreach (var type in assembly.GetTypes())
                {
                    if (baseEventType.IsAssignableFrom(type))
                    {
                        var attribute = type.GetCustomAttributes(attributeType, false).FirstOrDefault();
                        if (attribute != null && attribute is EventNameAttribute tCode
                                              && tCode.Code != default)
                        {
                            if (!this.codeDict.TryAdd(tCode.Code, type))
                            {
                                throw new TypeCodeRepeatedException(tCode.Code, type.FullName);
                            }

                            this.typeDict.TryAdd(type, tCode.Code);
                        }
                        else
                        {
                            this.typeDict.TryAdd(type, type.FullName);
                        }

                        if (!this.codeDict.TryAdd(type.FullName, type))
                        {
                            throw new TypeCodeRepeatedException(type.FullName, type.FullName);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 通过code获取Type对象
        /// </summary>
        /// <param name="typeCode"></param>
        /// <returns></returns>
        public Type FindType(string typeCode)
        {
            var value = this.codeDict.GetOrAdd(typeCode, key =>
            {
                foreach (var assembly in AssemblyHelper.GetAssemblies(this.logger))
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
            {
                throw new UnknownTypeCodeException(typeCode);
            }

            return value;
        }

        /// <summary>
        /// 获取Type对象的code字符串
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public string GetCode(Type type)
        {
            if (!this.typeDict.TryGetValue(type, out var value))
            {
                return type.FullName;
            }

            return value;
        }
    }
}