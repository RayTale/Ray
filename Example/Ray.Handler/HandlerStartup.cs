using Ray.Core.MQ;
using Ray.RabbitMQ;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Handler
{
    public class HandlerStartup
    {
        readonly ISubManager subManager;
        public HandlerStartup(ISubManager subManager)
        {
            this.subManager = subManager;
        }
        public Dictionary<SubscriberGroup, List<Subscriber>> Subscribers => new Dictionary<SubscriberGroup, List<Subscriber>>
        {
            {
                SubscriberGroup.Core,new List<Subscriber>
                {
                    new RabbitSubscriber(typeof(AccountCoreHandler) ,"Account", "account",  20)
                }
            },
            {
                SubscriberGroup.Rep,new List<Subscriber>
                {
                    new RabbitSubscriber(typeof(AccountRepHandler) ,"Account", "account",  20)
                }
            },
            {
                SubscriberGroup.Db,new List<Subscriber>
                {
                    new RabbitSubscriber(typeof(AccountToDbHandler) ,"Account", "account",  20)
                }
            }
        };
        public Task Start(SubscriberGroup group, string node = null, List<string> nodeList = null)
        {
            return subManager.Start(Subscribers[group], group.ToString(), node, nodeList);
        }
    }
    public enum SubscriberGroup
    {
        Core = 1,
        Rep = 2,
        Db = 3
    }
}
