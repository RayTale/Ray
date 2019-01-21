using Ray.Core.State;

namespace RayTest.IGrains.States
{
    public abstract class BaseState<K> : IState<K, StateBase<K>>
    {
        public abstract StateBase<K> Base { get; set; }
    }
}
