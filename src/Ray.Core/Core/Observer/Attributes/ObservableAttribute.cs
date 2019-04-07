using System;

namespace Ray.Core.Core.Observer
{
    /// <summary>
    /// 标记为可观察
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ObservableAttribute : Attribute
    {
    }
}
