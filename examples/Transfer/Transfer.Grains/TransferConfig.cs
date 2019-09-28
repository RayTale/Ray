using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.Core.Configuration;
using Ray.Core.Storage;
using Ray.Storage.PostgreSQL;
using Ray.Storage.SQLCore.Configuration;
using Transfer.Grains.Grains;

namespace Transfer.Grains
{
    public class TransferConfig : IStartupConfig
    {
        public void Configure(IServiceCollection serviceCollection)
        {
            serviceCollection.Configure<ArchiveOptions>(typeof(Account).FullName, options =>
            {
                options.On = true;
                options.SecondsInterval = 60;
                options.VersionInterval = 500;
            });
            serviceCollection.Configure<CoreOptions>(typeof(Account).FullName, options =>
            {
                options.ArchiveEventOnOver = true;
                options.PriorityAsyncEventBus = true;
            });
            serviceCollection.AddSingleton<IConfigureBuilder<long, Account>>(new PSQLConfigureBuilder<long, Account>((provider, id, parameter) =>new IntegerKeyOptions(provider, "core_event", "account")).AutoRegistrationObserver());

        }

        public Task ConfigureObserverUnit(IServiceProvider serviceProvider, IObserverUnitContainer followUnitContainer)
        {
            return Task.CompletedTask;
        }
    }
}
