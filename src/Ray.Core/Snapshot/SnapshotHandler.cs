using Ray.Core.Event;
using Ray.Core.Exceptions;
using System;
using System.Collections.Generic;
using System.Reflection.Emit;

namespace Ray.Core.Snapshot
{
    public class SnapshotHandler<PrimaryKey, Snapshot> : ISnapshotHandler<PrimaryKey, Snapshot>
         where Snapshot : class, new()
    {
        readonly Dictionary<Type, Action<Snapshot, IEvent>> _handlerDict = new Dictionary<Type, Action<Snapshot, IEvent>>();
        readonly Dictionary<Type, Action<Snapshot, IEvent, EventBase>> _handlerDict_1 = new Dictionary<Type, Action<Snapshot, IEvent, EventBase>>();
        readonly HandlerAttribute handlerAttribute;
        public SnapshotHandler()
        {
            var thisType = GetType();
            var handlerAttributes = thisType.GetCustomAttributes(typeof(HandlerAttribute), false);
            if (handlerAttributes.Length > 0)
                handlerAttribute = (HandlerAttribute)handlerAttributes[0];
            else
                handlerAttribute = default;
            foreach (var method in GetType().GetMethods())
            {
                var parameters = method.GetParameters();
                if (parameters.Length >= 2 &&
                    method.Name != nameof(Apply) &&
                    typeof(IEvent).IsAssignableFrom(parameters[1].ParameterType))
                {
                    if (!method.IsStatic)
                        throw new NotSupportedException("method must be static");
                    var evtType = parameters[1].ParameterType;
                    if (parameters.Length == 2)
                    {
                        var dynamicMethod = new DynamicMethod($"{evtType.Name}_handler", typeof(void), new Type[] { parameters[0].ParameterType, typeof(IEvent) }, thisType, true);
                        var ilGen = dynamicMethod.GetILGenerator();
                        ilGen.DeclareLocal(evtType);
                        ilGen.Emit(OpCodes.Ldarg_1);
                        ilGen.Emit(OpCodes.Castclass, evtType);
                        ilGen.Emit(OpCodes.Stloc_0);
                        ilGen.Emit(OpCodes.Ldarg_0);
                        ilGen.Emit(OpCodes.Ldloc_0);
                        ilGen.Emit(OpCodes.Call, method);
                        ilGen.Emit(OpCodes.Ret);
                        var func = (Action<Snapshot, IEvent>)dynamicMethod.CreateDelegate(typeof(Action<Snapshot, IEvent>));
                        _handlerDict.Add(evtType, func);
                    }
                    else if (parameters[2].ParameterType == typeof(EventBase))
                    {
                        var dynamicMethod = new DynamicMethod($"{evtType.Name}_handler", typeof(void), new Type[] { parameters[0].ParameterType, typeof(IEvent), typeof(EventBase) }, thisType, true);
                        var ilGen = dynamicMethod.GetILGenerator();
                        ilGen.DeclareLocal(evtType);
                        ilGen.Emit(OpCodes.Ldarg_1);
                        ilGen.Emit(OpCodes.Castclass, evtType);
                        ilGen.Emit(OpCodes.Stloc_0);
                        ilGen.Emit(OpCodes.Ldarg_0);
                        ilGen.Emit(OpCodes.Ldloc_0);
                        ilGen.Emit(OpCodes.Ldarg_2);
                        ilGen.Emit(OpCodes.Call, method);
                        ilGen.Emit(OpCodes.Ret);
                        var func = (Action<Snapshot, IEvent, EventBase>)dynamicMethod.CreateDelegate(typeof(Action<Snapshot, IEvent, EventBase>));
                        _handlerDict_1.Add(evtType, func);
                    }
                }
            }
        }
        public virtual void Apply(Snapshot<PrimaryKey, Snapshot> snapshot, IFullyEvent<PrimaryKey> fullyEvent)
        {
            var eventType = fullyEvent.Event.GetType();
            if (_handlerDict.TryGetValue(eventType, out var fun))
            {
                fun(snapshot.State, fullyEvent.Event);
            }
            else if (_handlerDict_1.TryGetValue(eventType, out var fun_1))
            {
                fun_1(snapshot.State, fullyEvent.Event, fullyEvent.Base);
            }
            else if (handlerAttribute == default || !handlerAttribute.Ignores.Contains(eventType))
            {
                throw new EventNotFoundHandlerException(eventType);
            }
        }
    }
}
