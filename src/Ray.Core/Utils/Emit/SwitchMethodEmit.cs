using System;
using System.Reflection;
using System.Reflection.Emit;

namespace Ray.Core.Utils.Emit
{
    /// <summary>
    /// 用来生成模式匹配方法调用的方法信息
    /// </summary>
    public class SwitchMethodEmit
    {
        /// <summary>
        /// 方法
        /// </summary>
        public MethodInfo Mehod { get; set; }
        /// <summary>
        /// 匹配的类型
        /// </summary>
        public Type CaseType { get; set; }
        /// <summary>
        /// 局部变量
        /// </summary>
        public LocalBuilder DeclareLocal { get; set; }
        /// <summary>
        /// 方法调用Lable
        /// </summary>
        public Label Lable { get; set; }
        /// <summary>
        /// 方法的参数
        /// </summary>
        public ParameterInfo[] Parameters { get; set; }
        /// <summary>
        /// 方法在类中的顺序
        /// </summary>
        public int Index { get; set; }
    }
}
