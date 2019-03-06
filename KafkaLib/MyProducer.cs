using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;

namespace KafkaLib
{
    public class MyProducer
    {
        private readonly string _broker;
        private readonly string _topic;
        private readonly string _kafkaLog;
        private readonly string _appLog;
        private static object _kafkaLocker = new object();
        private static object _logLocker = new object();

        public MyProducer(string broker, string topic, string kafkaLog, string appLog)
        {
            _broker = broker;
            _topic = topic;
            _kafkaLog = kafkaLog;
            _appLog = appLog;
        }

        public async Task<bool> SendMessage(string message)
        {
            #region 0.11.x
            var config = new Dictionary<string, object>
                {
                    {"bootstrap.servers", _broker},
                    {"message.timeout.ms", 5000}
                };

            var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));

            WriteAppLog($"Start to send message of [{message}]");
            await producer.ProduceAsync(_topic, null, JsonConvert.SerializeObject(message));
            WriteAppLog($"Finish to send message of [{message}]");
            #endregion

            #region 1.0 beta
            //var producerConfig = new ProducerConfig
            //{
            //    BootstrapServers = _broker,
            //    Debug = "all"
            //};

            //var producer = new ProducerBuilder<Null, string>(producerConfig).SetLogHandler((_, m) =>
            //{
            //    //if (!string.IsNullOrEmpty(_kafkaLog))
            //    //{
            //    //    lock (_kafkaLocker)
            //    //    {
            //    //        using (var file = new FileStream(_kafkaLog, FileMode.Append, FileAccess.Write, FileShare.Read))
            //    //        using (var writer = new StreamWriter(file, Encoding.Unicode))
            //    //        {
            //    //            writer.WriteLine($"{DateTime.Now}\t{m.Name}\t{m.Level}\t{m.Facility}\t{m.Message}");
            //    //        }
            //    //    }
            //    //}
            //}).Build();

            //var msg = new {Message = message, Time = DateTime.Now};

            //WriteAppLog($"Start to send message of [{message}]");

            //await producer.ProduceAsync(_topic, new Message<Null, string> { Value = JsonConvert.SerializeObject(msg) });

            //WriteAppLog($"Finish to send message of [{message}]");
            #endregion

            return await Task.FromResult(true);
        }

        private void WriteAppLog(string logText)
        {
            Console.WriteLine(logText);

            lock (_logLocker)
            {
                using (var file = new FileStream(_appLog, FileMode.Append, FileAccess.Write, FileShare.Read))
                using (var writer = new StreamWriter(file, Encoding.Unicode))
                {
                    writer.WriteLine($"{DateTime.Now}\t{logText}");
                }
            }
        }
    }
}
