using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Ray.Grain;
using Ray.Postgresql;
using Ray.RabbitMQ;
using Ray.IGrains;
using Ray.Core.Message;
using Orleans;
using System.Net;
using Orleans.Configuration;
using System.Collections.Generic;

namespace Ray.MongoHost
{
    class Program
    {
        static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }
        private static async Task<int> RunMainAsync()
        {
            try
            {
                var host = await StartSilo();

                Console.WriteLine("Input any key to stop");
                Console.ReadLine();
                await host.StopAsync();
                Console.WriteLine("Input any key to exit");

                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
        }
        private static async Task<ISiloHost> StartSilo()
        {
            var siloAddress = IPAddress.Loopback;

            var builder = new SiloHostBuilder()
                .UseLocalhostClustering()
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(Account).Assembly).WithReferences())
                .ConfigureServices((context, servicecollection) =>
                {
                    servicecollection.AddSingleton<ISerializer, ProtobufSerializer>();//注册序列化组件
                    servicecollection.AddPostgresql();//注册MongoDB为事件库
                    servicecollection.AddRabbitMQ<MessageInfo>();//注册RabbitMq为默认消息队列
                })
                .Configure<SqlConfig>(c =>
                {
                    c.ConnectionDict = new Dictionary<string, string> {
                        { "core_event","Server=127.0.0.1;Port=5432;Database=Ray;User Id=postgres;Password=luohuazhiyu;Pooling=true;MaxPoolSize=20;"}
                    };
                })
                .Configure<RabbitConfig>(c =>
                {
                    c.UserName = "admin";
                    c.Password = "luohuazhiyu";
                    c.Hosts = new[] { "127.0.0.1:5672" };
                    c.MaxPoolSize = 100;
                    c.VirtualHost = "/";
                })
               .ConfigureLogging(logging => logging.AddConsole());

            var host = builder.Build();
            await host.StartAsync();
            return host;
        }
    }
}
