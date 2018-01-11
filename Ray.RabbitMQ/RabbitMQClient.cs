using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using ProtoBuf;
using Ray.Core;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Lib;

namespace Ray.RabbitMQ
{
    public class ConnectionWrapper
    {
        public IConnection Connection { get; set; }
        int modelCount = 0;
        public int ModelCount { get { return modelCount; } }
        public int Increment()
        {
            return Interlocked.Increment(ref modelCount);
        }
        public void Decrement()
        {
            Interlocked.Decrement(ref modelCount);
        }
        public void Reset()
        {
            modelCount = 0;
        }
    }
    public class ModelWrapper : IDisposable
    {
        public ConnectionWrapper Connection { get; set; }
        public IModel Model { get; set; }
        public void Dispose()
        {
            RabbitMQClient.PushModel(this);
        }
    }
    public static class RabbitMQClient
    {
        static RabbitMQClient()
        {
            rabbitHost = Global.IocProvider.GetService<IOptions<RabbitConfig>>().Value;
            _Factory = new ConnectionFactory()
            {
                UserName = rabbitHost.UserName,
                Password = rabbitHost.Password,
                VirtualHost = rabbitHost.VirtualHost,
                AutomaticRecoveryEnabled = true
            };
        }
        static ConnectionFactory _Factory;
        static RabbitConfig rabbitHost;
        public static Task Publish<T>(this RabbitPubAttribute rabbitMQInfo, T data, string key)
        {
            return Publish(data, rabbitMQInfo.Exchange, rabbitMQInfo.GetQueue(key));
        }
        public static Task PublishByCmd<T>(this RabbitPubAttribute rabbitMQInfo, UInt16 cmd, T data, string key)
        {
            return PublishByCmd<T>(cmd, data, rabbitMQInfo.Exchange, rabbitMQInfo.GetQueue(key));
        }
        /// <summary>
        /// 发送消息到消息队列
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data"></param>
        /// <param name="exchange"></param>
        /// <param name="queue"></param>
        /// <returns></returns>
        public static Task Publish<T>(T data, string exchange, string queue, bool persistent = true)
        {
            byte[] msg;
            using (var ms = new PooledMemoryStream())
            {
                Serializer.Serialize(ms, data);
                msg = ms.ToArray();
            }
            return Publish(msg, exchange, queue, persistent);
        }
        public static Task PublishByCmd<T>(UInt16 cmd, T data, string exchange, string queue)
        {
            byte[] msg;
            using (var ms = new PooledMemoryStream())
            {
                ms.Write(BitConverter.GetBytes(cmd), 0, 2);
                Serializer.Serialize(ms, data);
                msg = ms.ToArray();
            }
            return Publish(msg, exchange, queue, false);
        }
        public static async Task Publish(byte[] msg, string exchange, string queue, bool persistent = true)
        {
            using (var channel = await PullModel())
            {
                var prop = channel.Model.CreateBasicProperties();
                prop.Persistent = persistent;
                channel.Model.BasicPublish(exchange, queue, prop, msg);
                channel.Model.WaitForConfirmsOrDie();
            }
        }
        public static async Task ExchangeDeclare(string exchange)
        {
            using (var channel = await PullModel())
            {
                channel.Model.ExchangeDeclare(exchange, "direct", true);
            }
        }
        static ConcurrentQueue<ModelWrapper> modelPool = new ConcurrentQueue<ModelWrapper>();
        static ConcurrentQueue<TaskCompletionSource<ModelWrapper>> modelTaskPool = new ConcurrentQueue<TaskCompletionSource<ModelWrapper>>();
        static ConcurrentBag<ConnectionWrapper> connectionList = new ConcurrentBag<ConnectionWrapper>();
        static int connectionCount = 0;
        static object modelLock = new object();
        public static IConnection CreateConnection()
        {
            return _Factory.CreateConnection(rabbitHost.EndPoints);
        }
        public static async Task<ModelWrapper> PullModel()
        {
            if (!modelPool.TryDequeue(out var model))
            {
                ConnectionWrapper conn = null;
                foreach (var item in connectionList)
                {
                    if (item.Increment() <= 15)
                    {
                        conn = item;
                        break;
                    }
                    else
                    {
                        item.Decrement();
                    }
                }
                if (conn == null && Interlocked.Increment(ref connectionCount) <= rabbitHost.MaxPoolSize)
                {
                    conn = new ConnectionWrapper() { Connection = _Factory.CreateConnection(rabbitHost.EndPoints) };
                    conn.Connection.ConnectionShutdown += (obj, args) =>
                    {
                        conn.Connection = _Factory.CreateConnection(rabbitHost.EndPoints);
                        conn.Reset();
                    };
                    connectionList.Add(conn);
                }
                if (conn != null)
                {
                    model = new ModelWrapper() { Connection = conn, Model = conn.Connection.CreateModel() };
                    model.Model.ConfirmSelect();
                }
                else
                {
                    Interlocked.Decrement(ref connectionCount);
                }

            }
            if (model == null)
            {
                var taskSource = new TaskCompletionSource<ModelWrapper>();
                modelTaskPool.Enqueue(taskSource);
                var cancelSource = new CancellationTokenSource(3000);
                cancelSource.Token.Register(() =>
                {
                    taskSource.SetException(new Exception("get rabbitmq's model timeout"));
                });
                model = await taskSource.Task;
            }
            if (model.Model.IsClosed)
            {
                model.Connection.Decrement();
                model = await PullModel();
            }
            return model;
        }
        public static void PushModel(ModelWrapper model)
        {
            if (model.Model.IsOpen)
            {
                if (modelTaskPool.Count > 0 && modelTaskPool.TryDequeue(out var modelTask))
                {
                    if (modelTask.Task.IsCanceled)
                        PushModel(model);
                    modelTask.SetResult(model);
                }
                else
                    modelPool.Enqueue(model);
            }
            else
            {
                model.Model.Dispose();
                model.Connection.Decrement();
            }
        }
    }
}
