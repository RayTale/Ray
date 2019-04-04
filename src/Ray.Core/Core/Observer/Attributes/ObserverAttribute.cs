using System;

namespace Ray.Core.Core.Observer
{
    /// <summary>
    /// 标记为观察者
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ObserverAttribute : Attribute
    {
        public ObserverAttribute(string group, Type observable, Type observer)
        {
            Group = group;
            Observable = observable;
            Observer = observer;
        }
        public string Group { get; set; }
        public Type Observable { get; set; }
        public Type Observer { get; set; }
    }
}
