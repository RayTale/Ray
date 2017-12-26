using Ray.Core.Message;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.MQ
{
    public abstract class PartSubHandler<K, TMessageWrapper> : ISubHandler where TMessageWrapper : MessageWrapper
    {
        Dictionary<string, Type> TypeFuncDict = new Dictionary<string, Type>();
        protected void Register<T>() where T : class
        {
            var type = typeof(T);
            TypeFuncDict.Add(type.FullName, type);
        }
        public virtual async Task Notice(byte[] dataBytes, TMessageWrapper message, object data)
        {
            if (data is IActorOwnMessage<K> @event)
                await Tell(dataBytes, @event, message);
        }
        public virtual Task Notice(byte[] bytes)
        {
            var serializer = Global.IocProvider.GetService<ISerializer>();
            using (var ms = new MemoryStream(bytes))
            {
                var msg = serializer.Deserialize<TMessageWrapper>(ms);
                if (TypeFuncDict.TryGetValue(msg.TypeCode, out var type))
                {
                    using (var ems = new MemoryStream(msg.BinaryBytes))
                    {
                        return Notice(bytes, msg, serializer.Deserialize(type, ems));
                    }
                }
            }
            return Task.CompletedTask;
        }
        public abstract Task Tell(byte[] bytes, IActorOwnMessage<K> data, TMessageWrapper msg);
    }
}
