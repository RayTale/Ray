using System;
using System.Threading.Tasks;
using Orleans;

namespace Ray.Core.Services
{
    public class WeightHoldLockGrain : Grain, IWeightHoldLock
    {
        private long lockId = 0;
        private long expireTime = 0;
        private int currentWeight = 0;
        private int maxWaitWeight = -1;

        public Task<bool> Hold(long lockId, int holdingSeconds = 30)
        {
            if (this.lockId == lockId && this.currentWeight >= this.maxWaitWeight)
            {
                this.expireTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + holdingSeconds * 1000;
                return Task.FromResult(true);
            }
            else
            {
                return Task.FromResult(false);
            }
        }

        public Task<(bool isOk, long lockId, int expectMillisecondDelay)> Lock(int weight, int holdingSeconds = 30)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (this.lockId == 0 || now > this.expireTime)
            {
                this.lockId = now;
                this.currentWeight = weight;
                this.maxWaitWeight = -1;
                this.expireTime = now + holdingSeconds * 1000;
                return Task.FromResult((true, now, 0));
            }

            if (weight >= this.maxWaitWeight && weight > this.currentWeight)
            {
                this.maxWaitWeight = weight;
                return Task.FromResult((false, (long)0, (int)(this.expireTime - now)));
            }

            return Task.FromResult((false, (long)0, 0));
        }

        public Task Unlock(long lockId)
        {
            if (this.lockId == lockId)
            {
                this.lockId = 0;
                this.currentWeight = 0;
                this.expireTime = 0;
            }

            return Task.CompletedTask;
        }
    }
}
