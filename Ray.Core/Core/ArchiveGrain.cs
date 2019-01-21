using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.Configuration;
using Ray.Core.Event;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;
using Ray.Core.Logging;
using Ray.Core.Serialization;
using Ray.Core.State;
using Ray.Core.Storage;
using Ray.Core.Utils;

namespace Ray.Core.Core
{
    public abstract class ArchiveGrain<K, E, S, B, W> : RayGrain<K, E, S, B, W>
        where E : IEventBase<K>
        where S : class, IState<K, B>, new()
        where B : IStateBase<K>, new()
        where W : IBytesWrapper, new()
    {
        public ArchiveGrain(ILogger logger) : base(logger)
        {
        }
        protected async ValueTask Arichive()
        {
            if (State.Base.IsOver)
                throw new StateIsOverException(State.Base.StateId.ToString(), GrainType);
            if (State.Base.Version != State.Base.DoingVersion)
                throw new StateInsecurityException(State.Base.StateId.ToString(), GrainType, State.Base.DoingVersion, State.Base.Version);
            //TODO 归档操作
        }
    }
}
