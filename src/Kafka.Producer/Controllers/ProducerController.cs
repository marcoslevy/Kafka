using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System;

namespace Kafka.Producer.Controllers
{
    [ApiController]
    [Route("api/[controller]")] 
    public class ProducerController : ControllerBase
    {
        [HttpPost]
        public IActionResult Post([FromQuery] string message)
        {
            return Created("", SendMessage(message));
        }

        private string SendMessage(string message)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var sendResult = producer
                        .ProduceAsync("filaTeste", new Message<Null, string> { Value = message })
                        .GetAwaiter()
                        .GetResult();

                    return $"Mensagem '{sendResult.Value}' de '{sendResult.TopicPartitionOffset}'";
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Erro ao enviar mensagem: {e.Error.Reason}");
                }
            }

            return string.Empty;
        }
    }
}
