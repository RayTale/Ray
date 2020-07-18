using System;
using System.Threading.Tasks;
using Orleans;

namespace Ray.Core.Services
{
    public class HoldLockGrain : Grain, IHoldLock
    {
        long lockId = 0;
        long expireTime = 0;
        public Task<(bool isOk, long lockId)> Lock(int holdingSeconds = 30)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (lockId == 0 || now > expireTime)
            {
                lockId = now;
                expireTime = now + holdingSeconds * 1000;
                return Task.FromResult((true, now));
            }
            else
            {
                return Task.FromResult((false, (long)0));
            }
        }
        public Task<bool> Hold(long lockId, int holdingSeconds = 30)
        {
            if (this.lockId == lockId)
            {
                expireTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + holdingSeconds * 1000;
                return Task.FromResult(true);
            }
            else
            {
                return Task.FromResult(false);
            }
        }

        public Task Unlock(long lockId)
        {
            if (this.lockId == lockId)
            {
                this.lockId = 0;
                expireTime = 0;
            }
            return Task.CompletedTask;
        }
    }
}
