using System;

namespace Ray.Core.Event
{
    /// <summary>
    /// TypeCode
    /// Event的类型唯一码标记，增加灵活性
    /// 默认使用Type.FullName作为TypeCode
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class EventNameAttribute : Attribute
    {
        public EventNameAttribute()
        {
        }

        public EventNameAttribute(string code) => Code = code;
        /// <summary>
        /// 类型唯一码
        /// </summary>
        public string Code { get; set; }
    }
}
