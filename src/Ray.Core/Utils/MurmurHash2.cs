using System.Runtime.InteropServices;

namespace Ray.Core.Utils
{
    public class MurmurHash2
    {
        public static uint Hash(byte[] data)
        {
            return Hash(data, 0xc58f1a7b);
        }
        const uint m = 0x5bd1e995;
        const int r = 24;

        [StructLayout(LayoutKind.Explicit)]
        struct BytetouintConverter
        {
            [FieldOffset(0)]
            public byte[] Bytes;

            [FieldOffset(0)]
            public uint[] UInts;
        }

        public static uint Hash(byte[] data, uint seed)
        {
            int length = data.Length;
            if (length == 0)
                return 0;
            uint h = seed ^ (uint)length;
            int currentIndex = 0;
            uint[] hackArray = new BytetouintConverter { Bytes = data }.UInts;
            while (length >= 4)
            {
                uint k = hackArray[currentIndex++];
                k *= m;
                k ^= k >> r;
                k *= m;

                h *= m;
                h ^= k;
                length -= 4;
            }
            currentIndex *= 4; // fix the length  
            switch (length)
            {
                case 3:
                    h ^= (ushort)(data[currentIndex++] | data[currentIndex++] << 8);
                    h ^= (uint)data[currentIndex] << 16;
                    h *= m;
                    break;
                case 2:
                    h ^= (ushort)(data[currentIndex++] | data[currentIndex] << 8);
                    h *= m;
                    break;
                case 1:
                    h ^= data[currentIndex];
                    h *= m;
                    break;
                default:
                    break;
            }

            // Do a few final mixes of the hash to ensure the last few  
            // bytes are well-incorporated.  

            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;
            return h;
        }
    }

}
