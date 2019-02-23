using System;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using Ray.Core.Services.Abstractions;
using System.Threading;

namespace Ray.Core.Services
{
    [Reentrant]
    public class UIDGrain : Grain, IUID
    {
        int newStringByUtcTimes = 1;
        long newStringByUtcStart = long.Parse(DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss"));
        readonly StringBuilder utcBuilder = new StringBuilder(22);
        public async Task<string> NewUtcID()
        {
            var nowTimestamp = long.Parse(DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss"));
            if (nowTimestamp > newStringByUtcStart)
            {
                Interlocked.Exchange(ref newStringByUtcStart, nowTimestamp);
                Interlocked.Exchange(ref newStringByUtcTimes, 0);
            }
            var newTimes = Interlocked.Increment(ref newStringByUtcTimes);
            if (newTimes <= 999999)
            {
                utcBuilder.Clear();
                utcBuilder.Append(newStringByUtcStart.ToString());
                var timesString = newTimes.ToString();
                for (int i = 0; i < 4 - timesString.Length; i++)
                {
                    utcBuilder.Append("0");
                }
                utcBuilder.Append(timesString);
                return utcBuilder.ToString();
            }
            else
            {
                await Task.Delay(1000);
                return await NewUtcID();
            }
        }

        int newStringByLocalTimes = 1;
        long newStringByLocalStart = long.Parse(DateTimeOffset.Now.ToString("yyyyMMddHHmmss"));
        readonly StringBuilder localBuilder = new StringBuilder(22);
        public async Task<string> NewLocalID()
        {
            var nowTimestamp = long.Parse(DateTimeOffset.Now.ToString("yyyyMMddHHmmss"));
            if (nowTimestamp > newStringByLocalStart)
            {
                Interlocked.Exchange(ref newStringByLocalStart, nowTimestamp);
                Interlocked.Exchange(ref newStringByLocalTimes, 0);
            }
            var newTimes = Interlocked.Increment(ref newStringByLocalTimes);
            if (newTimes <= 999999)
            {
                localBuilder.Clear();
                localBuilder.Append(newStringByLocalStart.ToString());
                var timesString = newTimes.ToString();
                for (int i = 0; i < 4 - timesString.Length; i++)
                {
                    localBuilder.Append("0");
                }
                localBuilder.Append(timesString);
                return localBuilder.ToString();
            }
            else
            {
                await Task.Delay(1000);
                return await NewLocalID();
            }
        }
    }
}
