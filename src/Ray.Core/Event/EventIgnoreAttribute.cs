using System;
using System.Collections.Generic;
using System.Linq;

namespace Ray.Core.Event
{
    /// <summary>
    /// EventHandler配置信息
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class EventIgnoreAttribute : Attribute
    {
        public EventIgnoreAttribute(params Type[] ignores)
        {
            this.Ignores = ignores.ToList();
        }

        /// <summary>
        /// 需要忽略的Event类型，不然系统会强制检查并抛出异常
        /// </summary>
        public List<Type> Ignores { get; set; }
    }
}
