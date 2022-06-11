using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Consumer
{
    public class MessageHandler : IHostedService
    {
        private readonly ILogger _logger;
        public MessageHandler(ILogger<MessageHandler> logger)
        {
            _logger = logger;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "filateste-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var item = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                item.Subscribe("filaTeste");
                var cts = new CancellationTokenSource();

                try
                {
                    while (true)
                    {
                        var message = item.Consume(cts.Token);
                        _logger.LogInformation($"Mensagem: {message.Value} recebida de {message.TopicPartition}");
                    }
                }
                catch (OperationCanceledException)
                {
                    item.Close();
                }
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
