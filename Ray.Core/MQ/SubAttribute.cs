using System;

namespace Ray.Core.MQ
{
    [AttributeUsage(AttributeTargets.Class)]
    public class SubAttribute : Attribute
    {
        public Type Handler { get; set; }
        public string Group { get; set; }
        public SubAttribute(string group) => Group = group;
    }
}
