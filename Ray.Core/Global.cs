using System;
using System.Collections.Generic;
using System.Text;

namespace Ray.Core
{
    public class Global
    {
        public static void Init(IServiceProvider provider)
        {
            IocProvider = provider;
        }
        public static IServiceProvider IocProvider { get; set; }
    }
}
