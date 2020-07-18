using System;
using System.Threading;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace Ray.Core.Services
{
    [Reentrant]
    public class UtcUIDGrain : Grain, IUtcUID
    {
        private int startId = 1;
        private string startString;
        private long startLong;
        private const int length = 19;

        public UtcUIDGrain()
        {
            this.startString = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
            this.startLong = long.Parse(this.startString); 
        }

        public Task<string> NewID()
        {
            return Task.FromResult(GenerateUtcId());
            string GenerateUtcId()
            {
                var now_string = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
                var now_Long = long.Parse(now_string);
                if (now_Long > this.startLong)
                {
                    Interlocked.Exchange(ref this.startString, now_string);
                    Interlocked.Exchange(ref this.startLong, now_Long);
                    Interlocked.Exchange(ref this.startId, 0);
                }

                var builder = new Span<char>(new char[length]);
                var newTimes = Interlocked.Increment(ref this.startId);
                if (newTimes <= 99999)
                {
                    this.startString.AsSpan().CopyTo(builder);

                    var timesString = newTimes.ToString();
                    for (int i = this.startString.Length; i < length - timesString.Length; i++)
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
