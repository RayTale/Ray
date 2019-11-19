using System;
using System.Collections.Generic;
using System.Linq;

namespace Ray.Core.Observer
{
    /// <summary>
    /// 标记为观察者
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ObserverAttribute : Attribute
    {
        /// <summary>
        /// 事件监听者标记
        /// </summary>
        /// <param name="group">监听者分组</param>
        /// <param name="name">监听者名称(如果是shadow请设置为null)</param>
        /// <param name="observable">被监听的Type</param>
        /// <param name="observer">监听者的Type</param>
        public ObserverAttribute(string group, string name, Type observable, Type observer = default)
        {
            Group = group;
            Name = name;
            Observable = observable;
            Observer = observer;
        }
        /// <summary>
        /// 监听者分组
        /// </summary>
        public string Group { get; set; }
        /// <summary>
        /// 监听者名称(如果是shadow请设置为null)
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// 被监听的Type
        /// </summary>
        public Type Observable { get; set; }
        /// <summary>
        /// 监听者的Type
        /// </summary>
        public Type Observer { get; set; }
    }
}
