using System;

namespace Ray.Storage.SQLCore.Configuration
{
    public class StringKeyOptions : StorageOptions
    {
        public StringKeyOptions(IServiceProvider serviceProvider, string connectionStr, string uniqueName, long subTableDaysInterval = 30, int stateIdLength = 200) : base(serviceProvider)
        {
            Connection = connectionStr;
            StateIdLength = stateIdLength;
            UniqueName = uniqueName;
            SubTableMillionSecondsInterval = subTableDaysInterval * 24 * 60 * 60 * 1000;
        }
        public int StateIdLength { get; set; }
    }
}
