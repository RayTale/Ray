using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.EventBus.RabbitMQ
{
    public class ConnectionWrapper
    {
        public IConnection Connection { get; set; }
        public IRabbitMQClient Client { get; set; }
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
        readonly IBasicProperties persistentProperties;
        readonly IBasicProperties noPersistentProperties;
        public ModelWrapper(ConnectionWrapper connectionWrapper, IModel model)
        {
            Connection = connectionWrapper;
            Model = model;
            persistentProperties = Model.CreateBasicProperties();
            persistentProperties.Persistent = true;
            noPersistentProperties = Model.CreateBasicProperties();
            noPersistentProperties.Persistent = false;
        }
        public ConnectionWrapper Connection { get; set; }
        public IModel Model { get; set; }
        public void Dispose()
        {
            Connection.Client.PushModel(this);
        }
        public void Publish(byte[] msg, string exchange, string routingKey, bool persistent = true)
        {
            Model.BasicPublish(exchange, routingKey, persistent ? persistentProperties : noPersistentProperties, msg);
        }
    }
    public class RabbitMQClient : IRabbitMQClient
    {
        readonly ConnectionFactory _Factory;
        readonly RabbitOptions rabbitHost;
        public RabbitMQClient(IOptions<RabbitOptions> config)
        {
            rabbitHost = config.Value;
            _Factory = new ConnectionFactory
            {
                UserName = rabbitHost.UserName,
                Password = rabbitHost.Password,
                VirtualHost = rabbitHost.VirtualHost,
                AutomaticRecoveryEnabled = false
            };
        }
        readonly ConcurrentQueue<ModelWrapper> modelPool = new ConcurrentQueue<ModelWrapper>();
        readonly List<ModelWrapper> modelList = new List<ModelWrapper>();
        readonly ConcurrentQueue<TaskCompletionSource<ModelWrapper>> modelTaskPool = new ConcurrentQueue<TaskCompletionSource<ModelWrapper>>();
        readonly ConcurrentQueue<ConnectionWrapper> connectionQueue = new ConcurrentQueue<ConnectionWrapper>();
        int connectionCount = 0;

        public async ValueTask<ModelWrapper> PullModel()
        {
            ConnectionWrapper GetConnection()
            {
                var fullList = new List<ConnectionWrapper>();
                while (connectionQueue.TryDequeue(out var connectionWrapper))
                {
                    if (connectionWrapper.Connection.IsOpen)
                    {
                        if (connectionWrapper.Increment() <= 16)
                        {
                            connectionQueue.Enqueue(connectionWrapper);
                            return connectionWrapper;
                        }
                        else
                        {
                            connectionWrapper.Decrement();
                            fullList.Add(connectionWrapper);
                        }
                    }
                }
                foreach (var item in fullList)
                {
                    connectionQueue.Enqueue(item);
                }
                return null;
            }
            if (!modelPool.TryDequeue(out var model))
            {
                var conn = GetConnection();
                if (conn == null && Interlocked.Increment(ref connectionCount) <= rabbitHost.MaxPoolSize)
                {
                    Task.Run(() =>
                    {
                        conn = new ConnectionWrapper
                        {
                            Client = this,
                            Connection = _Factory.CreateConnection(rabbitHost.EndPoints)
                        };
                        conn.Increment();
                        connectionQueue.Enqueue(conn);
                    }).GetAwaiter().GetResult();
                }
                else
                {
                    Interlocked.Decrement(ref connectionCount);
                }
                if (conn != null)
                {
                    model = new ModelWrapper(conn, conn.Connection.CreateModel());
                    model.Model.ConfirmSelect();
                    modelList.Add(model);
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
            return model;
        }
        public void PushModel(ModelWrapper model)
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
                modelList.Remove(model);
            }
        }

        public void Dispose()
        {
            foreach (var model in modelList)
            {
                model.Model.WaitForConfirmsOrDie();
            }
        }
    }
}
