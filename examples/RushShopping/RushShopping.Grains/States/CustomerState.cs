using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Ray.Core.Snapshot;
using RushShopping.Repository.Entities;

namespace RushShopping.Grains.States
{
    [Serializable]
    public class CustomerState : Customer,ICloneable<CustomerState>
    {
        #region Implementation of ICloneable<CustomerState>

        public CustomerState Clone()
        {
            using (var memoryStream = new MemoryStream())
            {
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(memoryStream, this);
                memoryStream.Position = 0;
                return (CustomerState) formatter.Deserialize(memoryStream);
            }
        }

        #endregion
    }
}