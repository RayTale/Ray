using System;

namespace Ray.Core.Abstractions.Observer
{
    /// <summary>
    /// Mark as observer
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ObserverAttribute : Attribute
    {
        public ObserverAttribute(DefaultName name, Type observable, Type observer = default)
            : this(GetGroup(name), name.ToString(), observable, observer)
        {
        }

        public ObserverAttribute(DefaultGroup group, DefaultName name, Type observable, Type observer = default)
           : this(group.ToString(), name.ToString(), observable, observer)
        {
        }

        /// <summary>
        /// Event listener mark
        /// </summary>
        /// <param name="group">Listener group</param>
        /// <param name="name">the name of the listener (if it is a shadow, please set it to null)</param>
        /// <param name="observable">Type being monitored</param>
        /// <param name="observer">Type of listener</param>
        public ObserverAttribute(string group, string name, Type observable, Type observer = default)
        {
            this.Group = group;
            this.Name = name;
            this.Observable = observable;
            this.Observer = observer;
        }

        private static string GetGroup(DefaultName name)
        {
            return name switch
            {
                DefaultName.Flow => DefaultGroup.Primary.ToString(),
                DefaultName.Shadow => DefaultGroup.Primary.ToString(),
                DefaultName.Db => DefaultGroup.Second.ToString(),
                _ => DefaultGroup.Third.ToString()
            };
        }

        /// <summary>
        /// Listener group
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        /// Listener name (if it is shadow, please set to null)
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Type being monitored
        /// </summary>
        public Type Observable { get; set; }

        /// <summary>
        /// Type of listener
        /// </summary>
        public Type Observer { get; set; }
    }
}