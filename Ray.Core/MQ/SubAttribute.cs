using System;

namespace Ray.Core.MQ
{
    [AttributeUsage(AttributeTargets.Class)]
    public class SubAttribute : Attribute
    {
        public Type Handler { get; set; }
        public string Type { get; set; }
        public SubAttribute(string type) => this.Type = type;
    }
}
