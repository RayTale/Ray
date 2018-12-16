using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Ray.Core.Messaging.Channels
{
    public interface IMpscChannelFactory<K, T> : IDisposable
    {
        IMpscChannel<T> Create(ILogger logger, K key, Func<List<T>, Task> consumer, int maxDataCountPerBatch = 5000, int minWaitMillisecondPerBatch = 100);
    }
}
