using Confluent.Kafka;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace KafkaConsole
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var consumerConfig = new ConsumerConfig
            {
                GroupId = $"dbserver1.dbo.Person.{Guid.NewGuid():N}.group.id",
                BootstrapServers = "192.168.0.29:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };


            using (var c = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                c.Subscribe("dbserver1.dbo.Person");
                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume();
                            var a = JsonSerializer.Serialize(cr.Message);
                            Mensagem b = null;

                            if (cr.Message.Value != null)
                                b = JsonSerializer.Deserialize<Mensagem>(cr.Message?.Value);

                            var chave = JsonSerializer.Deserialize<KeyIndentification>(cr.Key);
                            var offset = cr.TopicPartitionOffset;

                            if (b == null)
                            {
                                Console.WriteLine($"DELETADO - {chave.payload.Id}");
                            }
                            else
                            {
                                if (b.payload?.after != null && b.payload?.before == null)
                                    Console.WriteLine($"INCLUSÃO - ID:{b.payload?.after.Id}\nNOME:{b.payload?.after.Name}\nENDERECO:{b.payload?.after.Address}\nTELEFONE:{b.payload?.after.Phone}");
                                else
                                {
                                    if (b.payload?.after == null)
                                        Console.WriteLine($"ALTERAÇÃO - ID:{b.payload?.before.Id}\nNOME:{b.payload?.before.Name}\nENDERECO:{b.payload?.before.Address}\nTELEFONE:{b.payload?.before.Phone}");
                                    else
                                        Console.WriteLine($"ALTERAÇÃO - ID:{b.payload?.after.Id}\nNOME:{b.payload?.after.Name}\nENDERECO:{b.payload?.after.Address}\nTELEFONE:{b.payload?.after.Phone}");
                                }
                            }

                            Console.WriteLine("======================================================== \n");

                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            throw e;
                        }
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }
        }           
    }
}
