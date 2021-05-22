using RabbitMQ.Client;
using System;
using System.Text;

namespace RabbitMQ.Sample.Publisher
{

    class Program
    {
        public enum LogTypes
        {
            Ciritical = 1,
            Error = 2,
            Info = 3,
            Warning = 4
        }

        static void Main(string[] args)
        {

            //for (int i = 0; i < 100; i++)
            //{
            //    SendQueue(i.ToString());
            //}


            //for (int i = 0; i < 10; i++)
            //{
            //    FanoutExchangeSendQueue(i.ToString());
            //}

            //DirectExchangeSendQueue("1");


            TopicExchangeSendQueue("1");
            

            Console.WriteLine("Mesaj gönderildi.");
            Console.Read();
        }

        public static void SendQueue(string msg)
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

                string message = "Merhaba FATİH KOÇAK " + msg;
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(
                    exchange: string.Empty,
                    routingKey: queueName,
                    body: body
                    );
            }
        }

        public static void FanoutExchangeSendQueue(string msg)
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {

                channel.ExchangeDeclare(exchange: "logs", durable: true, type: ExchangeType.Fanout);
          
          
                string message = "Merhaba FATİH KOÇAK " + msg;
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("logs", routingKey: "", body: body);
            }
        }

        public static void DirectExchangeSendQueue(string msg)
        {

            var logNames = Enum.GetValues(typeof(LogTypes));

            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {

                channel.ExchangeDeclare(exchange: "system-logs", durable: true, type: ExchangeType.Direct);

                for (int i = 0; i < 11; i++)
                {
                    Random random = new Random();
                    LogTypes logType = (LogTypes)logNames.GetValue(random.Next(logNames.Length));
                    var body = Encoding.UTF8.GetBytes($"log={logType.ToString()}");
                    channel.BasicPublish("system-logs", routingKey: logType.ToString(), body: body);
                }

            }
        }

        public static void TopicExchangeSendQueue(string msg)
        {

            var logNames = Enum.GetValues(typeof(LogTypes));
            string routeKey = string.Empty;

            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {

                channel.ExchangeDeclare(exchange: "system-logs", durable: true, type: ExchangeType.Topic);
                for (int i = 0; i < 11; i++)
                {
                    Random random = new Random();
                    LogTypes logType1 = (LogTypes)logNames.GetValue(random.Next(logNames.Length));
                    LogTypes logType2 = (LogTypes)logNames.GetValue(random.Next(logNames.Length));
                    LogTypes logType3 = (LogTypes)logNames.GetValue(random.Next(logNames.Length));
                    routeKey = $"{logType1}.{logType2}.{logType3}";
                    string message = String.Concat("log=Test", i.ToString());
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish("system-logs", routingKey: routeKey, body: body);
                    Console.WriteLine($"Mesaj=>{message} <=> Routing Key=>{routeKey}");
                }

            }
        }


    }
}
