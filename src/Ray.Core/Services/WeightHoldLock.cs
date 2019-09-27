using System;
using System.Threading.Tasks;
using Orleans;

namespace Ray.Core.Services
{
    public class WeightHoldLock : Grain, IWeightHoldLock
    {
        long lockId = 0;
        long expireTime = 0;
        int currentWeight = 0;
        int maxWaitWeight = -1;

        public Task<bool> Hold(long lockId, int holdingSeconds = 30)
        {
            if (this.lockId == lockId && currentWeight >= maxWaitWeight)
            {
                expireTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + holdingSeconds * 1000;
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
            if (lockId == 0 || now > expireTime)
            {
                lockId = now;
                currentWeight = weight;
                maxWaitWeight = -1;
                expireTime = now + holdingSeconds * 1000;
                return Task.FromResult((true, now, 0));
            }
            if (weight >= maxWaitWeight && weight > currentWeight)
            {
                maxWaitWeight = weight;
                return Task.FromResult((false, (long)0, (int)(expireTime - now)));
            }
            return Task.FromResult((false, (long)0, 0));
        }

        public Task Unlock(long lockId)
        {
            if (this.lockId == lockId)
            {
                this.lockId = 0;
                currentWeight = 0;
                expireTime = 0;
            }
            return Task.CompletedTask;
        }
    }
}
