using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

namespace Ray.Core.MQ
{
    public interface ISubManager
    {
        Task Start(Assembly[] assemblys, string[] types = null, string node = null, List<string> nodeList = null);
    }
}
