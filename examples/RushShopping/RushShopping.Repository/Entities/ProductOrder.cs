using System;
namespace RushShopping.Repository.Entities
{
    [Serializable]
    public class ProductOrder
    {
        public ProductOrder()
        {
        }

        public Guid Id { get; set; }

        public decimal Price { get; set; }

        public Guid BuyerId { get; set; }
        public virtual Customer Customer { get; set; }

        public Guid ProductId { get; set; }
        public virtual Product Product { get; set; }
    }
}
