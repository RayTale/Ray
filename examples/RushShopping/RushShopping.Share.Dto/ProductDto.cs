using System;
using System.ComponentModel.DataAnnotations;

namespace RushShopping.Share.Dto
{
    public class ProductDto
    {
        public Guid Id { get; set; }

        [MaxLength(32)]
        public string Name { get; set; }

        public decimal Price { get; set; }

        public int TotalCount { get; set; }

        public int RemainsCount { get; set; }
    }
}