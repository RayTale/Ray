using System.Collections.Generic;

namespace RayTest.Grains
{
    public class ConnectionContainer
    {
        private Dictionary<string, string> _keyConnStrDic;
        public ConnectionContainer(/*DI conn string list*/)
        {
            //_keyConnStrDic =IOptions.Value.Opt();
        }
        public string GetConnStr(string key)
        {
            switch (key)
            {
                case "core_event":
                    return "core_event_Test_Connection_String";
                default: return string.Empty;
            }
        }
    }
}