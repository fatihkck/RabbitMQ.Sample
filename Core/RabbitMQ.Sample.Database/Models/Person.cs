using System;
using System.Collections.Generic;

#nullable disable

namespace RabbitMQ.Sample.Database.Models
{
    public partial class Person
    {
        public int Id { get; set; }
        public Guid? UniqueId { get; set; }
        public int? RowNo { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }
}
