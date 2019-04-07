﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Ray.EventBus.RabbitMQ
{
    public class ConsumerRunner
    {
        public ConsumerRunner(
            IRabbitMQClient client,
            ILogger<ConsumerRunner> logger,
            RabbitConsumer consumer,
            QueueInfo queue)
        {
            Client = client;
            Logger = logger;
            Consumer = consumer;
            Queue = queue;
        }
        public ILogger<ConsumerRunner> Logger { get; }
        public IRabbitMQClient Client { get; }
        public RabbitConsumer Consumer { get; }
        public QueueInfo Queue { get; }
        public ushort NowQos { get; set; }
        public List<ConsumerRunnerSlice> Slices { get; set; } = new List<ConsumerRunnerSlice>();
        public DateTimeOffset StartTime { get; set; }
        private bool isFirst = true;
        public async Task Run()
        {
            var child = new ConsumerRunnerSlice
            {
                Channel = await Client.PullModel(),
                Qos = Consumer.Config.MinQos
            };
            if (isFirst)
            {
                isFirst = false;
                child.Channel.Model.ExchangeDeclare(Consumer.EventBus.Exchange, "direct", true);
                child.Channel.Model.QueueDeclare(Queue.Queue, true, false, false, null);
                child.Channel.Model.QueueBind(Queue.Queue, Consumer.EventBus.Exchange, Queue.RoutingKey);
            }
            child.Channel.Model.BasicQos(0, Consumer.Config.MinQos, false);

            child.BasicConsumer = new EventingBasicConsumer(child.Channel.Model);
            child.BasicConsumer.Received += async (ch, ea) =>
            {
                await Process(child, ea, 0);
            };
            child.BasicConsumer.ConsumerTag = child.Channel.Model.BasicConsume(Queue.Queue, Consumer.Config.AutoAck, child.BasicConsumer);
            child.NeedRestart = false;
            Slices.Add(child);
            NowQos += child.Qos;
            StartTime = DateTimeOffset.UtcNow;
        }
        public async Task ExpandQos()
        {
            if (NowQos + Consumer.Config.IncQos <= Consumer.Config.MaxQos)
            {
                var child = new ConsumerRunnerSlice
                {
                    Channel = await Client.PullModel(),
                    Qos = Consumer.Config.IncQos
                };
                child.Channel.Model.BasicQos(0, Consumer.Config.IncQos, false);

                child.BasicConsumer = new EventingBasicConsumer(child.Channel.Model);
                child.BasicConsumer.Received += async (ch, ea) =>
                {
                    await Process(child, ea, 0);
                };
                child.BasicConsumer.ConsumerTag = child.Channel.Model.BasicConsume(Queue.Queue, Consumer.Config.AutoAck, child.BasicConsumer);
                child.NeedRestart = false;
                Slices.Add(child);
                NowQos += child.Qos;
                StartTime = DateTimeOffset.UtcNow;
            }
        }
        public async Task HeathCheck()
        {
            var unAvailables = Slices.Where(child => child.IsUnAvailable).ToList();
            if (unAvailables.Count > 0)
            {
                foreach (var slice in unAvailables)
                {
                    slice.Close();
                    Slices.Remove(slice);
                    NowQos -= slice.Qos;
                }
                if (NowQos < Consumer.Config.MinQos)
                {
                    await Run();
                }
            }
            else if ((DateTimeOffset.UtcNow - StartTime).TotalMinutes >= 5)
            {
                await ExpandQos();//扩容操作
            }
        }
        private async Task Process(ConsumerRunnerSlice consumerChild, BasicDeliverEventArgs ea, int count)
        {
            if (count > 0)
                await Task.Delay(count * 1000);
            try
            {
                await Consumer.Notice(ea.Body);
                if (!Consumer.Config.AutoAck)
                {
                    try
                    {
                        consumerChild.Channel.Model.BasicAck(ea.DeliveryTag, false);
                    }
                    catch
                    {
                        consumerChild.NeedRestart = true;
                    }
                }
            }
            catch (Exception exception)
            {
                Logger.LogError(exception.InnerException ?? exception, $"An error occurred in {Consumer.EventBus.Exchange}-{Queue}");
                if (Consumer.Config.Reenqueue)
                {
                    consumerChild.Channel.Model.BasicReject(ea.DeliveryTag, true);
                }
                else
                {
                    if (count > 3)
                        consumerChild.NeedRestart = true;
                    else
                        await Process(consumerChild, ea, count + 1);
                }
            }
        }
        public void Close()
        {
            foreach (var child in Slices)
            {
                child.Close();
            }
        }
    }
}
