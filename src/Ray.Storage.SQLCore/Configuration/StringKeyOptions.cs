using System;

namespace Ray.Storage.SQLCore.Configuration
{
    public class StringKeyOptions : StorageOptions
    {
        public StringKeyOptions(IServiceProvider serviceProvider, string connectionKey, string uniqueName, long subTableDaysInterval = 30, int stateIdLength = 200) : base(serviceProvider)
        {
            ConnectionKey = connectionKey;
            StateIdLength = stateIdLength;
            UniqueName = uniqueName;
            SubTableMillionSecondsInterval = subTableDaysInterval * 24 * 60 * 60 * 1000;
        }
        public int StateIdLength { get; set; }
    }
}
