using System.Threading.Tasks;

namespace Ray.Core.Channels
{
    public interface IBaseMpscChannel
    {
        /// <summary>
        /// 是否已经完成
        /// </summary>
        bool IsComplete { get; }
        /// <summary>
        /// 是否是子级channel
        /// </summary>
        bool IsChildren { get; set; }
        /// <summary>
        /// 把一个mpscchannel关联到另外一个mpscchannel，只要有消息进入，所有关联的channel都会顺序的进行消息检查和处理
        /// </summary>
        /// <param name="channel"></param>
        void JoinConsumerSequence(IBaseMpscChannel channel);
        /// <summary>
        /// 等待消息写入
        /// </summary>
        /// <returns></returns>
        Task<bool> WaitToReadAsync();
        Task ManualConsume();
        void Complete();
    }
}
