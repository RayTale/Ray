using System;

namespace Ray.Storage.SQLCore.Configuration
{
    public class StringKeyOptions : StorageOptions
    {
        public StringKeyOptions(IServiceProvider serviceProvider, string connectionStr, string uniqueName,
            long subTableDaysInterval = 30, int stateIdLength = 200) : base(serviceProvider)
        {
            StateIdLength = stateIdLength;
            Connection = connectionStr;
            UniqueName = uniqueName;
            SubTableMillionSecondsInterval = subTableDaysInterval * 24 * 60 * 60 * 1000;
        }

        /// <summary>
        /// Grain stateId max length
        /// </summary>
        public int StateIdLength { get; }
    }
}