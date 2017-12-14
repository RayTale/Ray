using Orleans;
using System;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.MQ;
using System.Threading.Tasks;

namespace Ray.Handler
{
    public class HandlerStart
    {
        public static IClusterClient Client { get; set; }
        public static Task Start(string[] types, IServiceProvider provider, IClusterClient client)
        {
            Client = client;
            var manager = provider.GetService<ISubManager>();
            return manager.Start(new[] { typeof(HandlerStart).Assembly }, types);
        }
    }
}
