using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ray.Core.Utils;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitPublisher
    {
        ConsistentHash _CHash;
        public IRabbitMQClient Client { get; set; }
        public bool CacheHashKey { get; set; }
        readonly Dictionary<string, ModelWrapper> models = new Dictionary<string, ModelWrapper>();
        public RabbitPublisher(string exchange = null, string queue = null, int queueCount = 1, bool cacheHashKey = false)
        {
            Exchange = exchange;
            Queue = queue;
            QueueCount = queueCount;
            CacheHashKey = cacheHashKey;
        }
        public string Exchange { get; set; }
        public string Queue { get; set; }
        public int QueueCount { get; set; }

        int isBuilded = 0;
        bool buildedResult = false;
        public async ValueTask Build(IRabbitMQClient client)
        {
            while (!buildedResult)
            {
                if (Interlocked.CompareExchange(ref isBuilded, 1, 0) == 0)
                {
                    try
                    {
                        await Init(client);
                        buildedResult = true;
                    }
                    finally
                    {
                        Interlocked.Exchange(ref isBuilded, 0);
                    }
                }
                await Task.Delay(50);
            }
        }
        private async Task Init(IRabbitMQClient client)
        {
            var isOnce = QueueCount == 1;
            if (isOnce)
            {
                var pullTask = client.PullModel();
                if (!pullTask.IsCompleted)
                    await pullTask;
                models.Add(Queue, pullTask.Result);
            }
            else
            {
                var nodeList = new List<string>();
                for (int i = 0; i < QueueCount; i++)
                {
                    var queue = $"{ Queue}_{i}";
                    nodeList.Add(queue);
                    if (!models.ContainsKey(queue))
                    {
                        var pullTask = client.PullModel();
                        if (!pullTask.IsCompleted)
                            await pullTask;
                        models.Add(queue, pullTask.Result);
                    }
                }
                _CHash = new ConsistentHash(nodeList, QueueCount * 10);
            }
            //申明exchange
            await client.ExchangeDeclare(Exchange);
            Client = client;
        }

        readonly ConcurrentDictionary<string, (string queue, ModelWrapper model)> queueDict = new ConcurrentDictionary<string, (string queue, ModelWrapper model)>();
        public async ValueTask<(string queue, ModelWrapper model)> GetQueue(string key)
        {
            if (CacheHashKey)
            {
                if (!queueDict.TryGetValue(key, out var result))
                {
                    var queue = QueueCount == 1 ? Queue : _CHash.GetNode(key);
                    result = (queue, models[queue]);
                    queueDict.TryAdd(key, result);
                }
                if (result.model.Model.IsClosed)
                {
                    result.model.Dispose();
                    var pullTask = Client.PullModel();
                    if (!pullTask.IsCompleted)
                        await pullTask;
                    result.model = models[result.queue] = pullTask.Result;
                }
                return result;
            }
            else
            {
                var queue = QueueCount == 1 ? Queue : _CHash.GetNode(key);
                var model = models[queue];
                if (model.Model.IsClosed)
                {
                    model.Dispose();
                    var pullTask = Client.PullModel();
                    if (!pullTask.IsCompleted)
                        await pullTask;
                    model = models[queue] = pullTask.Result;
                }
                return (queue, model);
            }
        }
    }
    public static class RabbitPublisherExtensions
    {
        public static async ValueTask Publish(this RabbitPublisher publisher, byte[] bytes, string key, bool persistent = true)
        {
            var task = publisher.GetQueue(key);
            if (!task.IsCompleted)
                await task;
            var (queue, model) = task.Result;
            model.Publish(bytes, publisher.Exchange, queue, persistent);
        }
        public static async ValueTask PublishByCmd<T>(this RabbitPublisher publisher, ushort cmd, T data, string key, bool persistent = true)
        {
            var task = publisher.GetQueue(key);
            if (!task.IsCompleted)
                await task;
            var (queue, model) = task.Result;
            model.PublishByCmd<T>(cmd, data, publisher.Exchange, queue, persistent);
        }
    }
}
