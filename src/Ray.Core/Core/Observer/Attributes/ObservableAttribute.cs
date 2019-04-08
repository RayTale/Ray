using System;

namespace Ray.Core
{
    /// <summary>
    /// 标记为可观察
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ObservableAttribute : Attribute
    {
    }
}
