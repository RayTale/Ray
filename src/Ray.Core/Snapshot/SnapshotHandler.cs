using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Utils.Emit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;

namespace Ray.Core.Snapshot
{
    public class SnapshotHandler<PrimaryKey, Snapshot> : ISnapshotHandler<PrimaryKey, Snapshot>
         where Snapshot : class, new()
    {
        readonly Action<object, Snapshot, IEvent, EventBase> handlerInvokeFunc;
        readonly HandlerAttribute handlerAttribute;
        public SnapshotHandler()
        {
            var thisType = GetType();
            var handlerAttributes = thisType.GetCustomAttributes(typeof(HandlerAttribute), false);
            if (handlerAttributes.Length > 0)
                handlerAttribute = (HandlerAttribute)handlerAttributes[0];
            else
                handlerAttribute = default;
            var methods = GetType().GetMethods().Where(m =>
            {
                var parameters = m.GetParameters();
                return parameters.Length >= 2 && parameters.Any(p => p.ParameterType == typeof(Snapshot)) && parameters.Any(p => typeof(IEvent).IsAssignableFrom(p.ParameterType));
            }).ToList();
            var dynamicMethod = new DynamicMethod($"Handler_Invoke", typeof(void), new Type[] { typeof(object), typeof(Snapshot), typeof(IEvent), typeof(EventBase) }, thisType, true);
            var ilGen = dynamicMethod.GetILGenerator();
            var items = new List<SwitchMethodEmit>();
            for (int i = 0; i < methods.Count; i++)
            {
                var method = methods[i];
                var methodParams = method.GetParameters();
                var caseType = methodParams.Single(p => typeof(IEvent).IsAssignableFrom(p.ParameterType)).ParameterType;
                items.Add(new SwitchMethodEmit
                {
                    Mehod = method,
                    CaseType = caseType,
                    DeclareLocal = ilGen.DeclareLocal(caseType),
                    Lable = ilGen.DefineLabel(),
                    Parameters = methodParams,
                    Index = i
                });
            }
            var defaultLabel = ilGen.DefineLabel();
            foreach (var item in items)
            {
                ilGen.Emit(OpCodes.Ldarg_2);
                ilGen.Emit(OpCodes.Isinst, item.CaseType);
                if (item.Index > 3)
                {
                    ilGen.Emit(OpCodes.Stloc_S, item.DeclareLocal);
                    ilGen.Emit(OpCodes.Ldloc_S, item.DeclareLocal);
                }
                else
                {
                    if (item.Index == 0)
                    {
                        ilGen.Emit(OpCodes.Stloc_0);
                        ilGen.Emit(OpCodes.Ldloc_0);
                    }
                    else if (item.Index == 1)
                    {
                        ilGen.Emit(OpCodes.Stloc_1);
                        ilGen.Emit(OpCodes.Ldloc_1);
                    }
                    else if (item.Index == 2)
                    {
                        ilGen.Emit(OpCodes.Stloc_2);
                        ilGen.Emit(OpCodes.Ldloc_2);
                    }
                    else
                    {
                        ilGen.Emit(OpCodes.Stloc_3);
                        ilGen.Emit(OpCodes.Ldloc_3);
                    }
                }

                ilGen.Emit(OpCodes.Brtrue_S, item.Lable);
            }
            ilGen.Emit(OpCodes.Br_S, defaultLabel);
            foreach (var item in items)
            {
                ilGen.MarkLabel(item.Lable);
                ilGen.Emit(OpCodes.Ldarg_0);
                //加载第一个参数
                if (item.Parameters[0].ParameterType == typeof(Snapshot))
                    ilGen.Emit(OpCodes.Ldarg_1);
                else if (item.Parameters[0].ParameterType == typeof(EventBase))
                    ilGen.Emit(OpCodes.Ldarg_3);
                else
                    LdEventArgs(item, ilGen);
                //加载第二个参数
                if (item.Parameters[1].ParameterType == typeof(Snapshot))
                    ilGen.Emit(OpCodes.Ldarg_1);
                else if (item.Parameters[1].ParameterType == typeof(EventBase))
                    ilGen.Emit(OpCodes.Ldarg_3);
                else
                    LdEventArgs(item, ilGen);
                //加载第三个参数
                if (item.Parameters.Length == 3)
                {
                    if (item.Parameters[1].ParameterType == typeof(Snapshot))
                        ilGen.Emit(OpCodes.Ldarg_1);
                    else if (item.Parameters[1].ParameterType == typeof(EventBase))
                        ilGen.Emit(OpCodes.Ldarg_3);
                    else
                        LdEventArgs(item, ilGen);
                }
                ilGen.Emit(OpCodes.Call, item.Mehod);
                ilGen.Emit(OpCodes.Ret);
            }
            ilGen.MarkLabel(defaultLabel);
            ilGen.Emit(OpCodes.Ldarg_0);
            ilGen.Emit(OpCodes.Ldarg_2);
            ilGen.Emit(OpCodes.Call, thisType.GetMethod(nameof(DefaultHandler)));
            ilGen.Emit(OpCodes.Ret);
            handlerInvokeFunc = (Action<object, Snapshot, IEvent, EventBase>)dynamicMethod.CreateDelegate(typeof(Action<object, Snapshot, IEvent, EventBase>));
            //加载Event参数
            static void LdEventArgs(SwitchMethodEmit item, ILGenerator gen)
            {
                if (item.Index > 3)
                {
                    gen.Emit(OpCodes.Ldloc_S, item.DeclareLocal);
                }
                else
                {
                    if (item.Index == 0)
                    {
                        gen.Emit(OpCodes.Ldloc_0);
                    }
                    else if (item.Index == 1)
                    {
                        gen.Emit(OpCodes.Ldloc_1);
                    }
                    else if (item.Index == 2)
                    {
                        gen.Emit(OpCodes.Ldloc_2);
                    }
                    else
                    {
                        gen.Emit(OpCodes.Ldloc_3);
                    }
                }
            }
        }
        public virtual void Apply(Snapshot<PrimaryKey, Snapshot> snapshot, IFullyEvent<PrimaryKey> fullyEvent)
        {
            handlerInvokeFunc(this, snapshot.State, fullyEvent.Event, fullyEvent.Base);
        }
        public void DefaultHandler(IEvent evt)
        {
            if (handlerAttribute == default || !handlerAttribute.Ignores.Contains(evt.GetType()))
            {
                throw new UnfindEventHandlerException(evt.GetType());
            }
        }
    }
}
