using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.WorkerService.Consumer
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        private ConnectionFactory _connectionFactory;
        private IConnection _connection;
        private IModel _channel;
        private const string QueueName = "Queue1";

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                _connectionFactory = new ConnectionFactory
                {
                    DispatchConsumersAsync = true,
                    HostName = "localhost",
                    VirtualHost = "production",
                    UserName = "guest",
                    Password = "123456"
                };
                _connection = _connectionFactory.CreateConnection();
                _channel = _connection.CreateModel();


                _channel.QueueDeclare(
                             queue: QueueName,
                             durable: true,
                             exclusive: false,
                             autoDelete: false,
                             arguments: null
                          );

                _logger.LogInformation($"Queue [{QueueName}] is waiting for messages.");

            }
            catch (BrokerUnreachableException e)
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

            return base.StartAsync(cancellationToken);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await base.StopAsync(cancellationToken);
            _connection.Close();
            _logger.LogInformation("RabbitMQ connection is closed.");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {



            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.Received += async (model, eArgs) =>
           {

               try
               {

                   _channel.BasicAck(eArgs.DeliveryTag, false);

                   //data processing iþlemi buradan sonra yapýlmalý
                   var body = eArgs.Body.ToArray();
                   var message = Encoding.UTF8.GetString(body);




                   //_logger.LogInformation("Mesaj alýndý");
                   Console.WriteLine("Mesaj alýndý");

                   await Task.Delay(50, stoppingToken);
                   //var result = GetMessageHandle(message);


               }
               catch (AlreadyClosedException ex)
               {
                   _logger.LogInformation("RabbitMQ is closed!", ex);
               }
               catch (Exception e)
               {
                   _logger.LogError(default, e, e.Message);
               }

           };

            _channel.BasicConsume(queue: QueueName,
                         consumer: consumer);

            await Task.CompletedTask;
            //while (!stoppingToken.IsCancellationRequested)
            //{
            //    _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            //    await Task.Delay(1000, stoppingToken);
            //}
        }
    }
}
