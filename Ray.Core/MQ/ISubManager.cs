using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.MQ
{
    public interface ISubManager
    {
        Task Start(string[] groups = null, string node = null, List<string> nodeList = null);
    }
}
