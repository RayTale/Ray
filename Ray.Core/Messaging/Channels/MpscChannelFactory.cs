using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Ray.Core.Messaging.Channels
{
    public class MpscChannelFactory<K, T> : IMpscChannelFactory<K, T>
    {
        readonly ConcurrentDictionary<K, MpscChannel<T>> channelDict = new ConcurrentDictionary<K, MpscChannel<T>>();
        readonly Timer monitorTimer;
        public MpscChannelFactory()
        {
            monitorTimer = new Timer(Monitor, null, 10 * 1000, 10 * 1000);
        }

        public IMpscChannel<T> Create(ILogger logger, K key, Func<List<T>, Task> consumer, int maxDataCountPerBatch = 5000)
        {
            return channelDict.GetOrAdd(key, k =>
            {
                return new MpscChannel<T>(logger, consumer, maxDataCountPerBatch);
            });
        }
        private void Monitor(object state)
        {
            var releasedList = new List<K>();
            foreach (var channel in channelDict)
            {
                if (channel.Value.IsComplete)
                    releasedList.Add(channel.Key);
                else if (!channel.Value.InConsuming)
                {
                    channel.Value.ActiveConsumer();
                }
            }
            foreach (var key in releasedList)
            {
                channelDict.TryRemove(key, out var _);
            }
        }
        public void Dispose()
        {
            monitorTimer.Dispose();
        }
    }
}
