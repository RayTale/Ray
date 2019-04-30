using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace RushShopping.Repository.Entities
{
    [Serializable]
    public class Customer : IEntity<Guid>
    {
        public Guid Id { get; set; }

        [MaxLength(32)] public string Name { get; set; }

        public decimal Balance { get; set; }

        public virtual ICollection<ProductOrder> ProductOrders { get; set; }
    }
}