using Ray.Core.EventBus;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitProducer : IProducer
    {
        readonly RabbitEventBus publisher;
        readonly IRabbitMQClient rabbitMQClient;
        public RabbitProducer(
            IRabbitMQClient rabbitMQClient,
            RabbitEventBus publisher)
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
                if (!pullTask.IsCompletedSuccessfully)
                    await pullTask;
                if (!modelDict.TryAdd(route, pullTask.Result))
                {
                    pullTask.Result.Dispose();
                }
                model = pullTask.Result;
            }
            else if (model.Model.IsClosed)
            {
                if (modelDict.TryRemove(route, out var value))
                {
                    value.Dispose();
                }
                var pullTask = PullModel(route);
                if (!pullTask.IsCompletedSuccessfully)
                    await pullTask;
                return pullTask.Result;
            }
            return model;
        }
        public async ValueTask Publish(byte[] bytes, string hashKey)
        {
            var route = publisher.GetRoute(hashKey);
            var pullTask = PullModel(route);
            if (!pullTask.IsCompletedSuccessfully)
                await pullTask;
            pullTask.Result.Publish(bytes, publisher.Exchange, route, publisher.Persistent);
        }
    }
}
