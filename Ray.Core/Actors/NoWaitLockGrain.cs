using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.Abstractions.Actors;

namespace Ray.Core.Actors
{
    public class NoWaitLockGrain : Grain, INoWaitLock
    {
        long id = 0;
        long expireTime = 0;
        public Task<(bool isOk, long lockId)> Lock(long holdingSeconds = 30)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (id == 0 || now > expireTime)
            {
                id = now;
                expireTime = now + holdingSeconds * 1000;
                return Task.FromResult((true, now));
            }
            else
            {
                return Task.FromResult((false, (long)0));
            }
        }
        public Task<bool> Hold(long lockId, long holdingSeconds = 30)
        {
            if (id == lockId)
            {
                expireTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + holdingSeconds * 1000;
                return Task.FromResult(true);
            }
            else
            {
                return Task.FromResult(false);
            }
        }

        public Task Unlock()
        {
            id = 0;
            expireTime = 0;
        }
    }
}
