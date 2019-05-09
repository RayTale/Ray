using System;
using Orleans;
using Ray.Core;

namespace RushShopping.IGrains
{
    public interface IProductDbGrain : IConcurrentObserver, IGrainWithGuidKey, ICrudDbGrain<Guid>
    {

    }
}