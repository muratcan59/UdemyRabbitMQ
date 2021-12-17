using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Shared;
using System.Text.Json;

namespace UdemyRabbitmq.publisher
{
    #region Notlar
    //Exchange: Var olan bir exchange varsa o yazılmalı, yoksa o kısım default exchange olarak ifade edilir.
    //Routing Key: Belirlenen kuyruk ifade edilir. Default exchange kullanılıyorsa belirlenen kuyruk yazılır.
    //Exchange tanımlı ise oradan gelen kuyruk ifade edilir.
    //MessageBody: Byte türülen oluşturulan mesaj atanır.
    //Durable: kuyruğun kayıt altına alınıp alınmayacağını belirler.
    //True olursa kuyruk sistem yeniden başlatıldığında otomatik olarak silinir.
    //False olursa sistemde fiziksel olarak kayıt edilip sistem yeniden başlatılsa dahi sistemden silinmez.
    //Exclusive: Farklı process(library) arasında erişim sağlama durumunu belirler.
    //True olursa sadece bulunduğu process(library) içinde çalışır.
    //False olursa diğer process ile etkileşim içine girebilir.
    #endregion

    //Direct - Topic Exchange Log İsimleri
    public enum LogNames
    {
        Critical = 1,
        Error = 2,
        Warning = 3,
        Info = 4
    }

    class Program
    {
        static void Main(string[] args)
        {
            #region Bağlantı Kurma Aşaması
            //RabbitMQ ile bağlantı kurulması için ConnectionFactory nesnesi eklendi.
            var factory = new ConnectionFactory();
            factory.Uri = new Uri("amqps://zxuhuhnz:uwT3kF1kowQ8859E7O4WgNOon-iqc9VJ@tiger.rmq.cloudamqp.com/zxuhuhnz"); //AMQP URL alındı

            //Eklenen nesne üzerinden RabbitMq Cloud hesabından AMQP URL alınarak bağlantı kuruldu.
            using var connection = factory.CreateConnection();

            //Oluşturulacak kuyruk/exchange için bir kanal(model) yaratıldı.
            var channel = connection.CreateModel();
            #endregion

            //Kuyruk Oluşturma

            #region Basit Kuyruk Oluşturma (Exchange Olmadan) 1
            //channel.QueueDeclare("hello-queue", true, false, false);
            //string message = "hello world";
            //var messageBody = Encoding.UTF8.GetBytes(message);
            //channel.BasicPublish(string.Empty, "hello-queue", null, messageBody);
            //Console.WriteLine("mesaj gönderilmiştir");
            #endregion

            #region Basit Kuyruk Oluşturma (Exchange Olmadan) 2
            //channel.QueueDeclare("hello-queue", true, false, false);
            //Enumerable.Range(1, 50).ToList().ForEach(x =>
            //{
            //    string message = $"Message {x}";
            //    var messageBody = Encoding.UTF8.GetBytes(message);
            //    channel.BasicPublish(string.Empty, "hello-queue", null, messageBody);
            //    Console.WriteLine($"Mesaj gönderilmiştir : {message}");
            //});
            #endregion

            //Exchange oluşturma

            #region Fanout Exchange
            //channel.ExchangeDeclare("logs-fanout", durable: true, type: ExchangeType.Fanout);
            //Enumerable.Range(1, 50).ToList().ForEach(x =>
            //{
            //    string message = $"log {x}";
            //    var messageBody = Encoding.UTF8.GetBytes(message);
            //    channel.BasicPublish("logs-fanout", "", null, messageBody);
            //    Console.WriteLine($"Mesaj gönderilmiştir : {message}");
            //});
            #endregion

            #region Direct Exchange
            //channel.ExchangeDeclare("logs-direct", durable: true, type: ExchangeType.Direct);
            //Enum.GetNames(typeof(LogNames)).ToList().ForEach(x =>
            //{
            //    var routeKey = $"route-{x}";
            //    var queueName = $"direct-queue-{x}";
            //    channel.QueueDeclare(queueName, true, false, false);
            //    channel.QueueBind(queueName, "logs-direct", routeKey, null);
            //});

            //Enumerable.Range(1, 50).ToList().ForEach(x =>
            //{
            //    LogNames log = (LogNames)new Random().Next(1, 5);
            //    string message = $"log-type: {log}";
            //    var messageBody = Encoding.UTF8.GetBytes(message);
            //    var routeKey = $"route-{log}";
            //    channel.BasicPublish("logs-direct", routeKey, null, messageBody);
            //    Console.WriteLine($"Log gönderilmiştir : {message}");
            //});
            #endregion

            #region Topic Exchange
            //channel.ExchangeDeclare("logs-topic", durable: true, type: ExchangeType.Topic);
            //Random rnd = new Random();
            //Enumerable.Range(1, 50).ToList().ForEach(x =>
            //{
            //    LogNames log1 = (LogNames)rnd.Next(1, 5);
            //    LogNames log2 = (LogNames)rnd.Next(1, 5);
            //    LogNames log3 = (LogNames)rnd.Next(1, 5);
            //    var routeKey = $"{log1}.{log2}.{log3}";
            //    string message = $"log-type: {log1}-{log2}-{log3}";
            //    var messageBody = Encoding.UTF8.GetBytes(message);
            //    channel.BasicPublish("logs-topic", routeKey, null, messageBody);  
            //    Console.WriteLine($"Log gönderilmiştir : {message}");
            //});
            #endregion

            #region Header Exchange
            //channel.ExchangeDeclare("header-exchange", durable: true, type: ExchangeType.Headers);          
            //Dictionary<string, object> headers = new Dictionary<string, object>();          
            //headers.Add("format", "pdf");
            //headers.Add("shape2", "a4"); --any durumuna uyan mesaj //headers.Add("shape", "a4"); --all durumuna uyan mesaj
            //var properties = channel.CreateBasicProperties();
            //properties.Headers = headers;
            //channel.BasicPublish("header-exchange", string.Empty, properties, Encoding.UTF8.GetBytes("header mesajım"));
            //Console.WriteLine("mesaj gönderilmiştir");
            #endregion

            #region Header Exchange - Complex Type
            channel.ExchangeDeclare("header-exchange", durable: true, type: ExchangeType.Headers);

            Dictionary<string, object> headers = new Dictionary<string, object>();

            headers.Add("format", "pdf");
            headers.Add("shape", "a4");

            var properties = channel.CreateBasicProperties();
            properties.Headers = headers;
            //Persistent Mesajları kalıcı hale getirmeyi sağlar. Diğer exchangelar da kullanılabilir.
            properties.Persistent = true;

            var product = new Product
            {
                Id = 1,
                Name = "Kalem",
                Price = 100,
                Stock = 10
            };

            var productJsonString = JsonSerializer.Serialize(product);

            channel.BasicPublish("header-exchange", string.Empty, properties, Encoding.UTF8.GetBytes(productJsonString));

            Console.WriteLine("mesaj gönderilmiştir");
            #endregion

            Console.ReadLine();
        }
    }
}
