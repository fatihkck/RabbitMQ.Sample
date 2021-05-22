using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQ.Sample.Consumer
{
    class Program
    {
        public enum SelectedLogTypes
        {
            Ciritical,
            Error,
        }

        static void Main(string[] args)
        {

            Console.WriteLine("Mesaj bekleniyor....");

            //GetMessage();

            //FanoutExchangeGetMessage();

            //DirectExchangeGetMessage();

            TopicExchangeGetMessage();

            Console.WriteLine("....");
            Console.Read();
        }

        public static void GetMessage()
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                /*
                 QueueDeclare Params Description:
                     Durable (the queue will survive a broker restart)
                     Exclusive (used by only one connection and the queue will be deleted when that connection closes)
                     Auto-delete (queue that has had at least one consumer is deleted when last consumer unsubscribes)
                     Arguments (optional; used by plugins and broker-specific features such as message TTL, queue length limit, etc) 
                 */
                string queueName = "QueueSample";
                channel.QueueDeclare(
                         queue: queueName,
                         durable: true,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null
                      );

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, eArgs) =>
                {
                    var body = eArgs.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    Thread.Sleep(2000);
                    Console.WriteLine("Gelen Mesaj {0}", message);
                };

                channel.BasicConsume(queue: queueName,
                             autoAck: true,
                             consumer: consumer);

                Console.WriteLine("Durdurmak için [enter]");
                Console.ReadLine();

            }
        }

        public static void FanoutExchangeGetMessage()
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {

                channel.ExchangeDeclare(exchange: "logs", durable: true, type: ExchangeType.Fanout);

                var queueName = channel.QueueDeclare().QueueName;
                channel.QueueBind(queue: queueName, exchange: "logs", routingKey: "");
                channel.BasicQos(0, 1, false);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, eArgs) =>
                {
                    var body = eArgs.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    Console.WriteLine("Gelen Mesaj {0}", message);
                };

                channel.BasicConsume(queue: queueName,
                             autoAck: true,
                             consumer: consumer);

                Console.WriteLine("Durdurmak için [enter]");
                Console.ReadLine();

            }
        }

        public static void DirectExchangeGetMessage()
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {

                channel.ExchangeDeclare(exchange: "system-logs", durable: true, type: ExchangeType.Direct);

                var queueName = channel.QueueDeclare().QueueName;
                foreach (var item in Enum.GetValues(typeof(SelectedLogTypes)))
                {
                    channel.QueueBind(queue: queueName, exchange: "system-logs", routingKey: item.ToString());
                }
                channel.BasicQos(0, 1, false);
                var consumer = new EventingBasicConsumer(channel);
                channel.BasicConsume(queueName, false, consumer: consumer);
                consumer.Received += (render, argument) =>
                {
                    string message = Encoding.UTF8.GetString(argument.Body.ToArray());
                    Console.WriteLine(message);
                    channel.BasicAck(deliveryTag: argument.DeliveryTag, false);

                };

                Console.WriteLine("Durdurmak için [enter]");
                Console.ReadLine();

            }
        }

        public static void TopicExchangeGetMessage()
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {

                channel.ExchangeDeclare(exchange: "system-logs", durable: true, type: ExchangeType.Topic);
                string routingKey = string.Empty;
                var queueName = channel.QueueDeclare().QueueName;

                routingKey = $"#.Warning";
                channel.QueueBind(queue: queueName, exchange: "system-logs", routingKey: routingKey);
                channel.BasicQos(0, 1, false);
                var consumer = new EventingBasicConsumer(channel);
                channel.BasicConsume(queueName, false, consumer: consumer);
                consumer.Received += (render, argument) =>
                {
                    string message = Encoding.UTF8.GetString(argument.Body.ToArray());
                    Console.WriteLine(message);
                    channel.BasicAck(deliveryTag: argument.DeliveryTag, false);

                };

                Console.WriteLine("Durdurmak için [enter]");
                Console.ReadLine();

            }
        }
    }
}
