using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Ray.Core.Utils
{
    public class ConsistentHash
    {
        private readonly SortedDictionary<int, string> circle = new SortedDictionary<int, string>();
        private int replicate = 200;    //default _replicate count
        private int[] ayKeys = null;    //cache the ordered keys for better performance

        //it's better you override the GetHashCode() of T.
        //we will use GetHashCode() to identify different node.
        public ConsistentHash(IEnumerable<string> nodes)
        {
            this.Init(nodes, this.replicate);
        }

        public ConsistentHash(IEnumerable<string> nodes, int replicate)
        {
            this.Init(nodes, replicate);
        }

        private void Init(IEnumerable<string> nodes, int replicate)
        {
            this.replicate = replicate;

            foreach (string node in nodes)
            {
                this.Add(node, false);
            }

            this.ayKeys = this.circle.Keys.ToArray();
        }

        public void Add(string node)
        {
            this.Add(node, true);
        }

        public void Add(string node, bool updateKeyArray)
        {
            for (int i = 0; i < this.replicate; i++)
            {
                int hash = BetterHash(node + i);
                this.circle[hash] = node;
            }

            if (updateKeyArray)
            {
                this.ayKeys = this.circle.Keys.ToArray();
            }
        }

        public void Remove(string node)
        {
            for (int i = 0; i < this.replicate; i++)
            {
                int hash = BetterHash(node + i);
                if (!this.circle.Remove(hash))
                {
                    throw new Exception("can not remove a node that not added");
                }
            }

            this.ayKeys = this.circle.Keys.ToArray();
        }

        public string GetNode(string key)
        {
            int first = First_ge(this.ayKeys, BetterHash(key));
            return this.circle[this.ayKeys[first]];
        }

        //return the index of first item that >= val.
        //if not exist, return 0;
        //ay should be ordered array.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int First_ge(int[] ay, int val)
        {
            int begin = 0;
            int end = ay.Length - 1;

            if (ay[end] < val || ay[0] > val)
            {
                return 0;
            }

            while (end - begin > 1)
            {
                int mid = (end + begin) / 2;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int BetterHash(string key)
        {
            return (int)MurmurHash2.Hash(Encoding.UTF8.GetBytes(key));
        }
    }
}
