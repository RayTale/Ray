using System;
using Microsoft.Extensions.Configuration;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using RushShopping.Repository;
using Microsoft.EntityFrameworkCore;

namespace RushShopping.Host
{
    class Program
    {
        public static IConfigurationRoot Configuration;
        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");
            Configuration = builder.Build();
        }

        public static IServiceProvider GetServiceProvider()
        {
            var services = new ServiceCollection();
            services.AddDbContext<RushShoppingDbContext>(option =>
            {
                option.UseNpgsql(Configuration.GetConnectionString("RushShoppingConnection"));
            }, ServiceLifetime.Transient)
            .AddEntityFrameworkNpgsql();
            return services.BuildServiceProvider();
        }
    }
}
