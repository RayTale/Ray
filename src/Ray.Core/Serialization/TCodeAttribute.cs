using System;

namespace Ray.Core.Serialization
{
    /// <summary>
    /// TypeCode
    /// Event的类型唯一码标记，增加灵活性
    /// 默认使用Type.FullName作为TypeCode
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class TCodeAttribute : Attribute
    {
        public TCodeAttribute(string code) => Code = code;
        /// <summary>
        /// 类型唯一码
        /// </summary>
        public string Code { get; set; }
    }
}
