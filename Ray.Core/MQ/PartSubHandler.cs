using Ray.Core.Message;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.MQ
{
    public abstract class PartSubHandler<TMessageWrapper> : ISubHandler where TMessageWrapper : MessageWrapper
    {
        Dictionary<string, SubFunc<TMessageWrapper>> TypeFuncDict = new Dictionary<string, SubFunc<TMessageWrapper>>();
        protected void Register<T>(Func<TMessageWrapper, object, Task> func) where T : class
        {
            var type = typeof(T);
            TypeFuncDict.Add(type.FullName, new SubFunc<TMessageWrapper> { MessageType = type, Func = func });
        }
        public virtual Task Notice(byte[] data)
        {
            var serializer = Global.IocProvider.GetService<ISerializer>();
            using (var ms = new MemoryStream(data))
            {
                var msg = serializer.Deserialize<TMessageWrapper>(ms);
                if (TypeFuncDict.TryGetValue(msg.TypeCode, out var func))
                {
                    using (var ems = new MemoryStream(msg.BinaryBytes))
                    {
                        return func.Func(msg, serializer.Deserialize(func.MessageType, ems));
                    }
                }
            }
            return Task.CompletedTask;
        }
    }
}
