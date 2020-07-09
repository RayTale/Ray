using System;

namespace Ray.Storage.SQLCore.Configuration
{
    public class IntegerKeyOptions : StorageOptions
    {
        public IntegerKeyOptions(IServiceProvider serviceProvider, string connectionStr, string uniqueName, long subTableDaysInterval = 30) : base(serviceProvider)
        {
            Connection = connectionStr;
            UniqueName = uniqueName;
            SubTableMillionSecondsInterval = subTableDaysInterval * 24 * 60 * 60 * 1000;
        }
    }
}
