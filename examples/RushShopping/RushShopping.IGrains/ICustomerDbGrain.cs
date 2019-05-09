using System;
using Orleans;
using Ray.Core;

namespace RushShopping.IGrains
{
    public interface ICustomerDbGrain : IConcurrentObserver, IGrainWithGuidKey, ICrudDbGrain<Guid>
    {
        
    }
}