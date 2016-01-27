#!/usr/bin/node

"use strict";

let amqp = require("amqplib"),
    url = "amqp://guest:guest@127.0.0.1:5672",
    rabbitMqConnection;

console.time('Creating connection');
amqp.connect(url)
    .then((connection) => {
      console.timeEnd('Creating connection');
      console.time('Creating channel');
      rabbitMqConnection = connection;
      return connection.createConfirmChannel();
    })
    .then((channel) => {
      console.timeEnd('Creating channel');
      console.time('Asserting exchanges');
      return Promise.all([
        /* Объявляем обменники с которыми мы будем работать */
        channel.assertExchange("billing", "topic", {durable: true}),
        channel.assertExchange("billing.dlx", "topic", {durable: true})
      ]).then(() => {
        console.timeEnd('Asserting exchanges');
        console.time('Asserting queues');
        return Promise.all([
          /* Объявляем очереди со всеми параметрами */
          channel.assertQueue("processOrders", {
            durable: true, arguments: {
              "x-dead-letter-exchange": "billing.dlx",
              "x-dead-letter-routing-key": "processOrders.dlx"
            }
          }),
          channel.assertQueue("processOrders.dlx", {
            durable: true, arguments: {
              "x-dead-letter-exchange": "billing",
              "x-message-ttl": 5000
            }
          }),
          channel.assertQueue("processOrders.after.3000", {
            durable: true, arguments: {
              "x-dead-letter-exchange": "billing",
              "x-message-ttl": 3000
            }
          }),
          channel.assertQueue("processOrders.after.2000", {
            durable: true, arguments: {
              "x-dead-letter-exchange": "billing",
              "x-message-ttl": 2000
            }
          }),
          channel.assertQueue("processOrders.after.1000", {
            durable: true, arguments: {
              "x-dead-letter-exchange": "billing",
              "x-message-ttl": 1000
            }
          })
        ]);
      }).then(() => {
        console.timeEnd('Asserting queues');
        console.time('Binding queues');
        /* Привязываем очереди к обменниками */
        return Promise.all([
          channel.bindQueue("processOrders", "billing", "processOrders.dlx"),
          channel.bindQueue("processOrders", "billing", "processOrders.after.*"),
          channel.bindQueue("processOrders", "billing", "processOrders"),
          channel.bindQueue("processOrders.dlx", "billing.dlx", "processOrders.dlx"),
          channel.bindQueue("processOrders.after.3000", "billing.dlx", "processOrders.after.3000"),
          channel.bindQueue("processOrders.after.2000", "billing.dlx", "processOrders.after.2000"),
          channel.bindQueue("processOrders.after.1000", "billing.dlx", "processOrders.after.1000")
        ]);
      }).then(() => {
        console.timeEnd('Binding queues');
        /* Возвращаем объект с каналом к RabbitMQ, чтобы можно было работать дальше с ним */
        return channel;
      });
    })
    .then((channel) => {
      console.time('Sending message');
      /* Отправляем сообщение в основную очередь */
      channel.publish("billing", "processOrders", new Buffer(JSON.stringify({
        orderId: Math.ceil(Math.random() * 10000)
      })), {
        persistent: true
      });
      console.timeEnd('Sending message');
      console.time('Closing channel');
      return channel.close().then(() => {
        console.timeEnd('Closing channel');
        return rabbitMqConnection.close();
      })
    })
    .then(process.exit);