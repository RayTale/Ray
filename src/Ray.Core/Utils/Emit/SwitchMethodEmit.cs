using System;
using System.Reflection;
using System.Reflection.Emit;

namespace Ray.Core.Utils.Emit
{
    /// <summary>
    /// Method information used to generate pattern matching method calls
    /// </summary>
    public class SwitchMethodEmit
    {
        /// <summary>
        /// Method
        /// </summary>
        public MethodInfo Method { get; set; }

        /// <summary>
        /// Matched type
        /// </summary>
        public Type CaseType { get; set; }

        /// <summary>
        /// local variables
        /// </summary>
        public LocalBuilder DeclareLocal { get; set; }

        /// <summary>
        /// Method call Label
        /// </summary>
        public Label Label { get; set; }

        /// <summary>
        /// Method parameters
        /// </summary>
        public ParameterInfo[] Parameters { get; set; }

        /// <summary>
        /// The order of the methods in the class
        /// </summary>
        public int Index { get; set; }
    }
}