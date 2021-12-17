using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;

namespace UdemyRabbitMQ.subscriber
{
    #region Notlar
    //Subscriber(Yayınlanan) tarafında kuyruk oluşturma zorunlu değildir.(QueueDeclare Metodu için)
    //Publisher(Yayınlayan) tarafında belirtilen kuyruk varsa hiçbir işlem yapmaz.
    //Yoksa belirtilen kuyruğu oluşturur. Eğer 2 tarafta tanımlı olup boolean özellikler farklı tanımlanırsa sistem hata verir.
    //Kuyruk sistem kapansa bile kayıt eder, silinmez.
    //AutoAck: Kuyruktaki mesajı silme durumunu belirler.True ise mesajı ilettiğinde kuyruk silinir. False ise silinmez.
    //Queue Bind(Metot): uygulama ayağa kalktığında kuyruk oluşacak. Kapandığında silinecek.
    //BasicQos(Metot): Mesajların subscriber(Yayınlanan) alana kaçar tane gideceği belirlenir.
    //Prefetch Size: Gönderilecek mesajların boyutu belirlenir. 0 verilirse gelen mesajların boyutuna bakmaz.
    //Prefetch Count: subscriber(Yayınlanan) alana 1 seferde kaç kuyruğun(mesajların) iletileceği belirlenir.
    //Global: Gönderilecek mesajların verilen mesaj gönderme sayısını parçalı veya tek seferde gönderimi belirlenir.
    //True ise subscriber(Yayınlanan) alanlara mesaj sayısı toplamı kadar iletilir.(Ör. 6 adet verilmişse 2 subscriber varsa her birine 6 6 olarak iletilir.)
    //False ise subscriber(Yayınlanan) alanlarla mesaj sayısı eşit şekilde dağıtılır.(Ör. 6 adet verilmişse 2 subscriber varsa birine 3 diğerine 3 olarak iletilir.)
    //BasicAck(Metot): Gelen mesajı silmek için yazılan metot
    //DeliveryTag: RabbitMQ ya hangi Tag ile ulaştırılan ilgili Tag üzerinden mesaj silinir.
    //Multiple: Gelen mesajın RabbitMQ ya iletilme durumu belirler. True ise iletir False ise iletmez.
    #endregion

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

            //Oluşturulacak kuyruk için bir kanal(model) yaratıldı.
            var channel = connection.CreateModel();
            #endregion

            #region Basit Kuyruk Oluşturma(Exchange Olmadan) 1
            //var consumer = new EventingBasicConsumer(channel);
            //channel.BasicConsume("hello-queue", false, consumer);
            //consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            //{
            //Byte türünden gelen mesaj string ile karşılandı.
            //   var message = Encoding.UTF8.GetString(e.Body.ToArray());
            //   Console.WriteLine("Gelen Mesaj:" + message);
            //}
            #endregion

            #region Basit Kuyruk Oluşturma(Exchange Olmadan) 2
            //channel.BasicQos(0, 1, false);
            //var consumer = new EventingBasicConsumer(channel);
            //channel.BasicConsume("hello-queue", false, consumer);
            //consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            //{
            //   var message = Encoding.UTF8.GetString(e.Body.ToArray());
            //   Thread.Sleep(1500);
            //   Console.WriteLine("Gelen Mesaj:" + message);
            //   channel.BasicAck(e.DeliveryTag, false);
            //}
            #endregion

            #region Fanout Exchange
            //var randomQueueName = channel.QueueDeclare().QueueName;
            //channel.QueueBind(randomQueueName, "logs-fanout", "", null);
            //channel.BasicQos(0, 1, false);
            //var consumer = new EventingBasicConsumer(channel);
            //channel.BasicConsume(randomQueueName, false, consumer);
            //Console.WriteLine("Logları dinleniyor... Çok yorulmuşlardı.");
            //consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            //{
            //   var message = Encoding.UTF8.GetString(e.Body.ToArray());
            //   Thread.Sleep(1500);
            //   Console.WriteLine("Gelen Mesaj:" + message);
            //   channel.BasicAck(e.DeliveryTag, false);
            //}
            #endregion

            #region Direct Exchange
            //channel.BasicQos(0, 1, false);
            //var consumer = new EventingBasicConsumer(channel);
            //var queueName = "direct-queue-Critical";
            //channel.BasicConsume(queueName, false, consumer);
            //Console.WriteLine("Logları dinleniyor... Çok yorulmuşlardı.");
            //consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            //{
            //   var message = Encoding.UTF8.GetString(e.Body.ToArray());
            //   Thread.Sleep(1500);
            //   Console.WriteLine("Gelen Mesaj:" + message);
            //Gelen mesaj log tutma
            //   File.AppendAllText("log-critical.txt", message + "\n");
            //   channel.BasicAck(e.DeliveryTag, false);
            //}
            #endregion

            #region Topic Exchange
            //channel.BasicQos(0, 1, false);
            //var consumer = new EventingBasicConsumer(channel);
            //Topic exchange da subscriber(yayınlanan) alanda hangileri alınacaksa o filtrelenir.
            //var routeKey = "*.Error.*";
            //var routeKey = "*.*.Warning";
            //var routeKey = "Info.#";
            //channel.QueueBind(queueName, "logs-topic", routeKey);
            //channel.BasicConsume(queueName, false, consumer);
            //Console.WriteLine("Logları dinleniyor... Çok yorulmuşlardı.");
            //consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            //{
            //   var message = Encoding.UTF8.GetString(e.Body.ToArray());
            //   Thread.Sleep(1500);
            //   Console.WriteLine("Gelen Mesaj:" + message);
            //   channel.BasicAck(e.DeliveryTag, false);
            //}
            #endregion

            #region Header Exchange
            //channel.BasicQos(0, 1, false);
            //var consumer = new EventingBasicConsumer(channel);
            //var queueName = channel.QueueDeclare().QueueName;
            //Dictionary<string, object> headers = new Dictionary<string, object>();
            //headers.Add("format", "pdf");
            //headers.Add("shape", "a4");
            //headers.Add("x-match", "any");--all tüm mesaj eşleşmesi, any herhangi bir mesaj eşleşmesi
            //channel.QueueBind(queueName, "header-exchange", String.Empty, headers);
            //channel.BasicConsume(queueName, false, consumer);
            //Console.WriteLine("Logları dinleniyor... Çok yorulmuşlardı.");
            //consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            //{
            //   var message = Encoding.UTF8.GetString(e.Body.ToArray());
            //   Thread.Sleep(1500);
            //   Console.WriteLine("Gelen Mesaj:" + message);
            //   channel.BasicAck(e.DeliveryTag, false);
            //}
            #endregion

            #region Header Exchange - Complex Type
            channel.BasicQos(0, 1, false);
            var consumer = new EventingBasicConsumer(channel);

            var queueName = channel.QueueDeclare().QueueName;

            Dictionary<string, object> headers = new Dictionary<string, object>();

            headers.Add("format", "pdf");
            headers.Add("shape", "a4");
            headers.Add("x-match", "any");

            channel.QueueBind(queueName, "header-exchange", String.Empty, headers);

            channel.BasicConsume(queueName, false, consumer);

            Console.WriteLine("Logları dinleniyor... Çok yorulmuşlardı.");

            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                //Byte türünden gelen mesaj string ile karşılandı.
                var message = Encoding.UTF8.GetString(e.Body.ToArray());

                Product product = JsonSerializer.Deserialize<Product>(message);

                Thread.Sleep(1500);
                Console.WriteLine($"Gelen Mesaj: {product.Id}-{product.Name}-{product.Price}-{product.Stock}");

                channel.BasicAck(e.DeliveryTag, false);
            };
            #endregion

            Console.ReadLine();
        }
    }
}
