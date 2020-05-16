using Ray.Core.EventBus;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.Abstractions
{
    public interface IObserverUnit<PrimaryKey> : IGrainID
    {
        /// <summary>
        /// 同步所有观察者
        /// </summary>
        /// <param name="primaryKey">GrainId</param>
        /// <param name="srcVersion">Observable的Version</param>
        /// <returns></returns>
        Task<bool[]> SyncAllObservers(PrimaryKey primaryKey, long srcVersion);
        /// <summary>
        /// 获取所有监听者分组
        /// </summary>
        /// <returns></returns>
        List<string> GetGroups();
        Task<long[]> GetAndSaveVersion(PrimaryKey primaryKey, long srcVersion);
        /// <summary>
        /// 重置Grain
        /// </summary>
        /// <param name="primaryKey">重置Grain</param>
        /// <returns></returns>
        Task Reset(PrimaryKey primaryKey);
        List<Func<BytesBox, Task>> GetEventHandlers(string observerGroup);
        List<Func<BytesBox, Task>> GetAllEventHandlers();
        List<Func<List<BytesBox>, Task>> GetBatchEventHandlers(string observerGroup);
        List<Func<List<BytesBox>, Task>> GetAllBatchEventHandlers();
    }
}
