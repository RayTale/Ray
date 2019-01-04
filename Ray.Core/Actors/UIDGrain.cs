using System;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using Ray.Core.Abstractions.Actors;

namespace Ray.Core.Actors
{
    [Reentrant]
    public class UIDGrain : Grain, IUID
    {
        int times = 1;
        long startTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        public async Task<long> NewLongID()
        {
            var nowTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (nowTimestamp > startTimestamp)
            {
                startTimestamp = nowTimestamp;
                times = 0;
            }
            times += 1;
            if (times <= 65535)
            {
                return (startTimestamp << 16) | (ushort)times;
            }
            else
            {
                await Task.Delay(1);
                return await NewLongID();
            }
        }
        int newStringByUtcTimes = 1;
        long newStringByUtcStart = long.Parse(DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmssfff"));
        readonly StringBuilder utcBuilder = new StringBuilder(22);
        public async Task<string> NewUtcID()
        {
            var nowTimestamp = long.Parse(DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmssfff"));
            if (nowTimestamp > newStringByUtcStart)
            {
                newStringByUtcStart = nowTimestamp;
                newStringByUtcTimes = 0;
            }
            newStringByUtcTimes += 1;
            if (newStringByUtcTimes <= 99999)
            {
                utcBuilder.Clear();
                utcBuilder.Append(newStringByUtcStart.ToString());
                var timesString = newStringByUtcTimes.ToString();
                for (int i = 0; i < 4 - timesString.Length; i++)
                {
                    utcBuilder.Append("0");
                }
                utcBuilder.Append(timesString);
                return utcBuilder.ToString();
            }
            else
            {
                await Task.Delay(1);
                return await NewUtcID();
            }
        }

        int newStringByLocalTimes = 1;
        long newStringByLocalStart = long.Parse(DateTime.Now.ToString("yyyyMMddHHmmssfff"));
        readonly StringBuilder localBuilder = new StringBuilder(22);
        public async Task<string> NewLocalID()
        {
            var nowTimestamp = long.Parse(DateTime.Now.ToString("yyyyMMddHHmmssfff"));
            if (nowTimestamp > newStringByLocalStart)
            {
                newStringByLocalStart = nowTimestamp;
                newStringByLocalTimes = 0;
            }
            newStringByLocalTimes += 1;
            if (newStringByLocalTimes <= 99999)
            {
                localBuilder.Clear();
                localBuilder.Append(newStringByLocalStart.ToString());
                var timesString = newStringByLocalTimes.ToString();
                for (int i = 0; i < 4 - timesString.Length; i++)
                {
                    localBuilder.Append("0");
                }
                localBuilder.Append(timesString);
                return localBuilder.ToString();
            }
            else
            {
                await Task.Delay(1);
                return await NewLocalID();
            }
        }
    }
}
