using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ray.Core.Utils
{
    public class ConsistentHash
    {
        readonly SortedDictionary<int, string> circle = new SortedDictionary<int, string>();
        int _replicate = 200;    //default _replicate count
        int[] ayKeys = null;    //cache the ordered keys for better performance

        //it's better you override the GetHashCode() of T.
        //we will use GetHashCode() to identify different node.
        public ConsistentHash(IEnumerable<string> nodes)
        {
            Init(nodes, _replicate);
        }

        public ConsistentHash(IEnumerable<string> nodes, int replicate)
        {
            Init(nodes, replicate);
        }
        private void Init(IEnumerable<string> nodes, int replicate)
        {
            _replicate = replicate;

            foreach (string node in nodes)
            {
                this.Add(node, false);
            }
            ayKeys = circle.Keys.ToArray();
        }

        public void Add(string node)
        {
            Add(node, true);
        }

        private void Add(string node, bool updateKeyArray)
        {
            for (int i = 0; i < _replicate; i++)
            {
                int hash = BetterHash(node + i);
                circle[hash] = node;
            }

            if (updateKeyArray)
            {
                ayKeys = circle.Keys.ToArray();
            }
        }

        public void Remove(string node)
        {
            for (int i = 0; i < _replicate; i++)
            {
                int hash = BetterHash(node + i);
                if (!circle.Remove(hash))
                {
                    throw new Exception("can not remove a node that not added");
                }
            }
            ayKeys = circle.Keys.ToArray();
        }

        //we keep this function just for performance compare
        private string GetNode_slow(String key)
        {
            int hash = BetterHash(key);
            if (circle.ContainsKey(hash))
            {
                return circle[hash];
            }

            int first = circle.Keys.FirstOrDefault(h => h >= hash);
            if (first == new int())
            {
                first = ayKeys[0];
            }
            string node = circle[first];
            return node;
        }

        //return the index of first item that >= val.
        //if not exist, return 0;
        //ay should be ordered array.
        int First_ge(int[] ay, int val)
        {
            int begin = 0;
            int end = ay.Length - 1;

            if (ay[end] < val || ay[0] > val)
            {
                return 0;
            }

            int mid = begin;
            while (end - begin > 1)
            {
                mid = (end + begin) / 2;
                if (ay[mid] >= val)
                {
                    end = mid;
                }
                else
                {
                    begin = mid;
                }
            }

            if (ay[begin] > val || ay[end] < val)
            {
                throw new Exception("should not happen");
            }

            return end;
        }

        public string GetNode(String key)
        {
            //return GetNode_slow(key);

            int hash = BetterHash(key);

            int first = First_ge(ayKeys, hash);

            //int diff = circle.Keys[first] - hash;

            return circle[ayKeys[first]];
        }

        //default String.GetHashCode() can't well spread strings like "1", "2", "3"
        public static int BetterHash(string key)
        {
            uint hash = MurmurHash2.Hash(Encoding.UTF8.GetBytes(key));
            return (int)hash;
        }
    }

}
