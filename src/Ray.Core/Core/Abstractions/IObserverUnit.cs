using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Abstractions;

namespace Ray.Core
{
    public interface IObserverUnit<PrimaryKey> : IGrainID
    {
        /// <summary>
        /// 获取所有监听者分组
        /// </summary>
        /// <returns></returns>
        List<string> GetGroups();
        Task<long[]> GetAndSaveVersion(PrimaryKey primaryKey, long srcVersion);
        List<Func<byte[], Task>> GetEventHandlers(string observerGroup);
        List<Func<byte[], Task>> GetAllEventHandlers();
    }
}
