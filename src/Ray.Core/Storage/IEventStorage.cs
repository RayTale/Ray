using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Event;

namespace Ray.Core.Storage
{
    public interface IEventStorage<PrimaryKey>
    {
        /// <summary>
        /// Single event insertion
        /// </summary>
        /// <param name="fullyEvent"></param>
        /// <param name="eventJson"></param>
        /// <param name="unique"></param>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        Task<bool> Append(FullyEvent<PrimaryKey> fullyEvent, string eventJson, string unique);

        /// <summary>
        /// Batch event insertion
        /// </summary>
        /// <param name="list"></param>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        Task TransactionBatchAppend(List<EventBox<PrimaryKey>> list);

        /// <summary>
        /// Get events in batch
        /// </summary>
        /// <param name="stateId">State Id, equivalent to GrainId</param>
        /// <param name="latestTimestamp">the starting time of the list to be pulled</param>
        /// <param name="startVersion">start version</param>
        /// <param name="endVersion">End version</param>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        Task<IList<FullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion);

        /// <summary>
        /// Get events of specified type in batch
        /// </summary>
        /// <param name="stateId">State Id, equivalent to GrainId</param>
        /// <param name="typeCode">the starting time of the list to be pulled</param>
        /// <param name="startVersion">start version</param>
        /// <param name="limit">pull quantity</param>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        Task<IList<FullyEvent<PrimaryKey>>> GetListByType(PrimaryKey stateId, string typeCode, long startVersion, int limit);

        /// <summary>
        /// Delete events before the specified version number
        /// </summary>
        /// <param name="stateId">State Id, equivalent to GrainId</param>
        /// <param name="toVersion">End version number</param>
        /// <param name="startTimestamp">the start timestamp of the current deletion</param>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        Task DeletePrevious(PrimaryKey stateId, long toVersion, long startTimestamp);

        /// <summary>
        /// Delete events after the specified version number
        /// </summary>
        /// <param name="stateId">State Id, equivalent to GrainId</param>
        /// <param name="fromVersion">End version number</param>
        /// <param name="startTimestamp">the start timestamp of the current deletion</param>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        Task DeleteAfter(PrimaryKey stateId, long fromVersion, long startTimestamp);

        /// <summary>
        /// Delete the event of the specified version number
        /// </summary>
        /// <param name="stateId">State ID</param>
        /// <param name="version">version number</param>
        /// <param name="timestamp">Time stamp of the event</param>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        Task DeleteByVersion(PrimaryKey stateId, long version, long timestamp);
    }
}