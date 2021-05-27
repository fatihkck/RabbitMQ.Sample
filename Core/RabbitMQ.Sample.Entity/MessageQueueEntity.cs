using System;

namespace RabbitMQ.Sample.Entity
{
    public class MessageQueueEntity
    {
        public Guid UniqueId { get; set; }
        public int RowNo { get; set; }
        public string RegisterNumber { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public int Age { get; set; }
        public string Email { get; set; }
        public string Phone { get; set; }
        public string Webs { get; set; }
        public string PhotosUrl { get; set; }
        public string FacebookUrl { get; set; }
        public string LinkedinUrl { get; set; }
        public string Twitter { get; set; }
        public string Country { get; set; }
        public string City { get; set; }
        public string Adress { get; set; }
        public string Description { get; set; }
        public string Skills { get; set; }

    }
}
