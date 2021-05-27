using RabbitMQ.Client;
using RabbitMQ.Sample.Entity;
using RabbitMQ.Sample.Library.Cache;
using RabbitMQ.Sample.Library.Cache.Redis;
using StackExchange.Redis;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;

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

            Console.WriteLine("Queue ismi giriniz...");
            var queName = Console.ReadLine();
            Console.WriteLine("{0} Mesaj gönderiliyor....", queName);

            //SetRedisMessage("keys_name", "message");


            var messageDataList = new MessageQueueEntityLoad().Gets();


            foreach (var item in messageDataList)
            {

                var jsonMsg = JsonSerializer.Serialize(item);

                try
                {
                    Thread.Sleep(100);
                    SendQueue(jsonMsg, queName);
                }
                catch (Exception ex)
                {

                    Console.WriteLine(ex.Message);
                    Console.Read();
                }

                //SendQueue(item.UniqueId.ToString());
                //SetRedisMessage(item.UniqueId.ToString(), jsonMsg);

                Console.WriteLine(messageDataList.IndexOf(item));
            }

            Console.WriteLine("Mesaj gönderildi.");
            Console.Read();
        }

        public static void SendQueue(string msg, string queueName)
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

                //string queueName = "QueueSample";
                channel.QueueDeclare(
                         queue: queueName,
                         durable: false,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null
                        );

                string message = msg;
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(
                    exchange: string.Empty,
                    routingKey: queueName,
                    body: body
                    );
            }
        }

        /// <summary>
        /// Pub/Sub işlemleri, kuyruktaki datayı paralel olarak işletmek istediğinde kullanılabilinir.
        /// orn: yüklenen bir video 480,720,1080 vb çözünürlüklerine göre aynı anda encode edilmeye başlatılabilinir.
        /// </summary>
        /// <param name="msg"></param>
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


        #region Helper 

        public static void SetRedisMessage(string key, string message)
        {

            RedisCacheManager manager = new RedisCacheManager();
            manager.Set(key, message);




            //using (ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost"))
            //{
            //    // ^^^ store and re-use this!!!

            //    IDatabase db = redis.GetDatabase();
            //    db.StringSet(key, Encoding.UTF8.GetBytes(message));
            //}

            //string value = db.StringGet("mykey");
            //            Console.WriteLine(value); // writes: "abcdefg"

            //using (IRedisNativeClient client = new RedisClient())
            //{

            //    //var clientQueue = client.As<MessageQueueEntity>();

            //    //var savedCustomer = clientQueue.Store(queueEntity);
            //    client.Set(key, Encoding.UTF8.GetBytes(message));

            //    //string result = Encoding.UTF8.GetString(client.Get("Mesaj:1"));

            //    //   client.Del("Mesaj:1");
            //    //Console.WriteLine($"Değer : {result}");
            //}
        }
        #endregion

    }
}
