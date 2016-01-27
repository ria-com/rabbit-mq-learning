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
      /* Объявляем обменник с которым будем работать */
      return channel.assertExchange("billing", "topic", {durable: true})
          .then(() => {
            console.timeEnd('Asserting exchanges');
            console.time('Asserting queues');
            /* Объявляем очередь */
            return channel.assertQueue("processOrders", {
              durable: true, arguments: {
                "x-dead-letter-exchange": "billing.dlx",
                "x-dead-letter-routing-key": "processOrders.dlx"
              }
            });
          }).then(() => {
            console.timeEnd('Asserting queues');
            console.time('Binding queues');
            /* Привязываем очередь к обменнику */
            return Promise.all([
              channel.bindQueue("processOrders", "billing", "processOrders.dlx"),
              channel.bindQueue("processOrders", "billing", "processOrders.after.*"),
              channel.bindQueue("processOrders", "billing", "processOrders")
            ])
          }).then(() => {
            console.timeEnd('Binding queues');
            return channel;
          });
    })
    .then((channel) => {
      channel.prefetch(1).then(() => {
        channel.consume("processOrders", (msg) => {
          let messageContent;
          try {
            messageContent = JSON.parse(msg.content.toString());
          } catch (e) {
            console.log("consumer.js:53 --> ", e);
          }
          if (messageContent) {
            let actionId = messageContent.orderId % 10;
            switch (actionId) {
              case 1:
              case 2:
              case 3:
                if (msg.properties.headers["x-death"]) {
                  console.log("consumer.js:62 --> Сообщение уже было у нас в на обработке. ", JSON.stringify(msg.properties.headers["x-death"]));
                } else {
                  /* Переставляем в другую очередь */
                  channel.publish("billing.dlx", `processOrders.after.${actionId * 1000}`, new Buffer(JSON.stringify(messageContent)), {
                    persistent: true
                  });
                }
                channel.ack(msg);
                break;
              default:
                if (msg.properties.headers["x-death"]) {
                  console.log("consumer.js:73 --> Сообщение уже было у нас в на обработке. ", JSON.stringify(msg.properties.headers["x-death"]));
                } else {
                  /* Сообщение "умрет" и попадет в обменник и очередь, указанные в параметрах x-dead-letter-exchange и x-dead-letter-routing-key */
                  channel.nack(msg, false, false);
                }
                break;
            }
          } else {
            channel.ack(msg);
          }
        });
      })
    });