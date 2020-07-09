using System;

namespace Ray.Storage.SQLCore.Configuration
{
    public class IntegerKeyOptions : StorageOptions
    {
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="serviceProvider"></param>
        /// <param name="connectionStr">连接字符串</param>
        /// <param name="uniqueName">Grain名称</param>
        /// <param name="subTableDaysInterval"></param>
        public IntegerKeyOptions(IServiceProvider serviceProvider, string connectionStr, string uniqueName,
            long subTableDaysInterval = 30) : base(serviceProvider)
        {
            UniqueName = uniqueName;
            Connection = connectionStr;
            SubTableMillionSecondsInterval = subTableDaysInterval * 24 * 60 * 60 * 1000;
        }
    }
}