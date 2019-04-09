using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace RushShopping.Share.Dto
{
    public class CustomerDto
    {
        public Guid Id { get; set; }

        [MaxLength(32)] public string Name { get; set; }

        public decimal Balance { get; set; }


        public virtual ICollection<ProductOrderDto> ProductOrders { get; set; }
    }
}