using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Ray.Grain;
using Ray.MongoES;
using Ray.RabbitMQ;
using Ray.IGrains;
using Ray.Core.Message;
using Orleans;

namespace Ray.Host
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

                Console.WriteLine("Press Enter to terminate...");

                Console.ReadLine();

                await host.StopAsync();

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
            var builder = new SiloHostBuilder()
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(Account).Assembly).WithReferences())
                .ConfigureLocalHostPrimarySilo()
                .ConfigureServices((context, servicecollection) =>
                {
                    servicecollection.AddSingleton<ISerializer, ProtobufSerializer>();//注册序列化组件
                    servicecollection.AddMongoES();//注册MongoDB为事件库
                    servicecollection.AddRabbitMQ<MessageInfo>();//注册RabbitMq为默认消息队列
                })
                .Configure<MongoConfig>(c =>
                {
                    c.SysStartTime = new DateTime(2018, 2, 1);
                    c.Connection = "mongodb://127.0.0.1:28888";
                })
                .Configure<RabbitConfig>(c =>
                {
                    c.UserName = "admin";
                    c.Password = "luohuazhiyu";
                    c.Hosts = new[] { "192.168.199.216:5672" };
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
