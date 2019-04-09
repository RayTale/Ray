using System;

namespace RushShopping.Share.Dto
{
    public class ProductOrderDto
    {

        public Guid Id { get; set; }

        public decimal Price { get; set; }

        public Guid BuyerId { get; set; }
        public virtual CustomerDto Customer { get; set; }

        public Guid ProductId { get; set; }
        public virtual ProductDto Product { get; set; }
    }
}