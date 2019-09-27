using Orleans;
using Orleans.Concurrency;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.Core.Services
{
    [Reentrant]
    public class UtcUIDGrain : Grain, IUtcUID
    {
        int start_id = 1;
        string start_string;
        long start_long;
        const int length = 19;
        public UtcUIDGrain()
        {
            start_string = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
            start_long = long.Parse(start_string); 
        }
        public Task<string> NewID()
        {
            return Task.FromResult(GenerateUtcId());
            string GenerateUtcId()
            {
                var now_string = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
                var now_Long = long.Parse(now_string);
                if (now_Long > start_long)
                {
                    Interlocked.Exchange(ref start_string, now_string);
                    Interlocked.Exchange(ref start_long, now_Long);
                    Interlocked.Exchange(ref start_id, 0);
                }
                var builder = new Span<char>(new char[length]);
                var newTimes = Interlocked.Increment(ref start_id);
                if (newTimes <= 99999)
                {
                    start_string.AsSpan().CopyTo(builder);

                    var timesString = newTimes.ToString();
                    for (int i = start_string.Length; i < length - timesString.Length; i++)
                    {
                        builder[i] = '0';
                    }
                    var span = length - timesString.Length;
                    for (int i = span; i < length; i++)
                    {
                        builder[i] = timesString[i - span];
                    }
                    return builder.ToString();
                }
                else
                {
                    return GenerateUtcId();
                }
            }
        }
    }
}
