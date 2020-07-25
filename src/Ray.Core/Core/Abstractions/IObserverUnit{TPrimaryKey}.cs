using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.EventBus;

namespace Ray.Core.Abstractions
{
    public interface IObserverUnit<in TPrimaryKey> : IObserverUnit
    {
        /// <summary>
        /// Synchronize all observers
        /// </summary>
        /// <param name="primaryKey">GrainId</param>
        /// <param name="srcVersion">Observable of Version</param>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        Task<bool[]> SyncAllObservers(TPrimaryKey primaryKey, long srcVersion);

        /// <summary>
        /// Get all listener groups
        /// </summary>
        /// <returns></returns>
        List<string> GetGroups();

        Task<long[]> GetAndSaveVersion(TPrimaryKey primaryKey, long srcVersion);

        /// <summary>
        /// Reset Grain
        /// </summary>
        /// <param name="primaryKey">Reset Grain</param>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        Task Reset(TPrimaryKey primaryKey);

        List<Func<BytesBox, Task>> GetEventHandlers(string observerGroup);

        List<Func<BytesBox, Task>> GetAllEventHandlers();

        List<Func<List<BytesBox>, Task>> GetBatchEventHandlers(string observerGroup);

        List<Func<List<BytesBox>, Task>> GetAllBatchEventHandlers();

        /// <summary>
        /// Get the group of the Observer
        /// </summary>
        /// <param name="observerType"></param>
        /// <returns></returns>
        string GetGroup(Type observerType);
    }
}