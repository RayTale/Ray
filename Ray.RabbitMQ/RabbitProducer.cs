using System.Collections.Concurrent;
using System.Threading.Tasks;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitProducer<W> : IProducer 
        where W : IBytesWrapper
    {
        readonly RabbitEventBus<W> publisher;
        readonly IRabbitMQClient rabbitMQClient;
        public RabbitProducer(
            IRabbitMQClient rabbitMQClient,
            RabbitEventBus<W> publisher)
        {
            this.publisher = publisher;
            this.rabbitMQClient = rabbitMQClient;
        }

        readonly ConcurrentDictionary<string, ModelWrapper> modelDict = new ConcurrentDictionary<string, ModelWrapper>();
        public async ValueTask<ModelWrapper> PullModel(string route)
        {
            if (!modelDict.TryGetValue(route, out var model))
            {
                var pullTask = rabbitMQClient.PullModel();
                if (!pullTask.IsCompleted)
                    await pullTask;
                if (!modelDict.TryAdd(route, pullTask.Result))
                {
                    pullTask.Result.Dispose();
                }
            }
            else if (model.Model.IsClosed)
            {
                if (modelDict.TryRemove(route, out var value))
                {
                    value.Dispose();
                }
                var pullTask = PullModel(route);
                if (!pullTask.IsCompleted)
                    await pullTask;
                return pullTask.Result;
            }
            return model;
        }
        public async ValueTask Publish(byte[] bytes, string hashKey)
        {
            var route = publisher.GetRoute(hashKey);
            var pullTask = PullModel(route);
            if (!pullTask.IsCompleted)
                await pullTask;
            pullTask.Result.Publish(bytes, publisher.Exchange, route, false);
        }
    }
}
