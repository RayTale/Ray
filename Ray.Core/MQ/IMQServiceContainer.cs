using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ray.Core.MQ
{
    public interface IMQServiceContainer
    {
        IMQService GetService(Type type);
    }
}
