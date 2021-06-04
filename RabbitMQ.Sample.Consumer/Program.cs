using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Sample.Database.Models;
using RabbitMQ.Sample.Entity;
using RabbitMQ.Sample.Library.Cache.Redis;
using System;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
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

        static Stopwatch stopwatch = null;
        static int rowNo = 10;

        static void Main(string[] args)
        {

            Console.WriteLine("Queue ismi giriniz...");
            var queName = Console.ReadLine();

            Console.WriteLine("{0} Mesaj bekleniyor....", queName);

            GetMessage(queName);

            Console.WriteLine("....");
            Console.Read();
        }

        public static void GetMessage(string queueName)
        {
            //stopwatch = new Stopwatch();
            //stopwatch.Start();

            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";
            factory.UserName = "guest";
            factory.Password = "123456";
            factory.VirtualHost = "production";


            try
            {



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
                             durable: true,
                             exclusive: false,
                             autoDelete: false,
                             arguments: null
                          );

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, eArgs) =>
                    {

                        try
                        {
                            //once state processing olarak update edilmeli sonra acknowledgement edilmeli                           
                            channel.BasicAck(eArgs.DeliveryTag, false);

                            //data processing işlemi buradan sonra yapılmalı
                            var body = eArgs.Body.ToArray();
                            var message = Encoding.UTF8.GetString(body);

                            var result = GetMessageHandle(message);

                            //Thread.Sleep(1000);
                            //PRECONDITION-FAILED hatası için her zaman act edilmeli
                            //https://www.grzegorowski.com/rabbitmq-406-channel-closed-precondition-failed
                            //Already closed: The AMQP operation was interrupted: AMQP close-reason, initiated by Peer, code=406, text='PRECONDITION_FAILED - consumer ack timed out on channel 1', classId=0, methodId=0'
                        }
                        catch (Exception ex)
                        {

                            ///RabbitMQ.Client.Exceptions.AlreadyClosedException
                            Console.WriteLine("Received event err {0}", ex.Message);
                        }

                    };

                    channel.BasicConsume(queue: queueName,
                                 consumer: consumer);
                    //channel.BasicConsume(queue: queueName,
                    //             autoAck: true,
                    //             consumer: consumer);

                    Console.WriteLine("Durdurmak için [enter]");
                    Console.ReadLine();

                }


            }
            catch (Client.Exceptions.BrokerUnreachableException e)
            {
                Console.WriteLine("Broker unreachable err {0}", e.Message);
                Console.ReadLine();

                //Thread.Sleep(5000);
                // apply retry logic
                //connection fail ise yeniden connect olunabilinir
            }
            catch (Exception ex)
            {

                Console.WriteLine("General err {0}", ex.Message);
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


        #region Helper
        public static bool GetMessageHandle(string message)
        {
            try
            {
                var queueEntity = JsonSerializer.Deserialize<MessageQueueEntity>(message);

                Console.WriteLine("{0} {1} {2} ", queueEntity.UniqueId, queueEntity.RowNo, queueEntity.Name);

                TableSave(queueEntity.UniqueId, queueEntity.RowNo, queueEntity.Name, queueEntity.Description);

                //if (queueEntity.RowNo == MessageQueueEntityLoad.RowCount)
                //{
                //    stopwatch.Stop();
                //    Console.WriteLine("Time elapsed: {0:hh\\:mm\\:ss}", stopwatch.Elapsed);
                //}
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message);
                Debug.WriteLine(ex.Message);
                Console.ReadLine();
                return false;
            }

            return true;
        }

        public static bool GetMessageHandleWithRedis(string uniqueId)
        {
            try
            {
                var message = GetRedisMessage(uniqueId);

                var queueEntity = JsonSerializer.Deserialize<MessageQueueEntity>(message);

                Console.WriteLine("{0} {1} ", queueEntity.UniqueId, queueEntity.RowNo);

                if (queueEntity.RowNo == MessageQueueEntityLoad.RowCount)
                {
                    stopwatch.Stop();
                    Console.WriteLine("Time elapsed: {0:hh\\:mm\\:ss}", stopwatch.Elapsed);
                }
            }
            catch (Exception ex)
            {

                Debug.WriteLine(ex.Message);
                return false;
            }

            return true;
        }

        public static string GetRedisMessage(string key)
        {


            RedisCacheManager manager = new RedisCacheManager();
            string result = manager.Get(key);
            var removeResult = manager.Remove(key);

            return result;

            //using (IRedisNativeClient client = new RedisClient())
            //{

            //    //var clientQueue = client.As<MessageQueueEntity>();

            //    //var savedCustomer = clientQueue.Store(queueEntity);


            //    string result = Encoding.UTF8.GetString(client.Get(key));
            //    client.Del(key);

            //    return result;
            //    //   client.Del("Mesaj:1");
            //    //Console.WriteLine($"Değer : {result}");
            //}

            return string.Empty;
        }

        public static void TableSave(Guid uniqueId, int rowNo, string name, string description)
        {
            using (SampleContext db = new SampleContext())
            {

                Person person = new Person();
                person.UniqueId = uniqueId;
                person.RowNo = rowNo;
                person.Name = name;
                person.Description = description;

                db.Add(person);
                db.SaveChanges();
            }
        }
        #endregion
    }
}
