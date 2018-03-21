using Orleans;
using System;

namespace Ray.Core.MQ
{
    public interface IMQServiceContainer
    {
        IMQService GetService(Type type, Grain grain);
    }
}
