//
// Created by ytymchenko on 21.03.2021.
//
#ifndef SHEEPPL_PARALLEL_FOR_H
#define SHEEPPL_PARALLEL_FOR_H

#include "RabbitClient.h"
#include "blocked_range.h"
#include "utils.h"

namespace sheeppl {


    template<typename Range, typename Body>
    void parallel_for(const Range &range, const Body &body, RabbitClient &client) {
        client.channel_consume.declareQueue("rpc_queue");
        client.receive() ? receive(range, body, client) : split_exec(range, body, client);
    }

    template<typename Range, typename Body>
    void split_exec(const Range &range, const Body &body, RabbitClient &client) {
        if (!range.is_divisible()) {
            std::cout << range.begin() << " " << range.end() << std::endl;
            exec_task(range, body, client);
            return;
        }
        auto range_pair = range.split_();
        split_exec(range_pair.first, body, client);
        split_exec(range_pair.second, body, client);
    };

    template<typename Range, typename Body>
    void exec_task(const Range &range, const Body &body, RabbitClient &client) {
        auto data_string = serialize(range);
        AMQP::Envelope env(data_string.c_str(), data_string.size());
        client.channel_consume.publish("", "rpc_queue", env);
    }

    template<typename Range, typename Body>
    void receive(const Range &range, const Body &body, RabbitClient &client) {
        client.channel_consume.declareQueue("rpc_queue");
        client.channel_consume.consume("").onReceived([&](const AMQP::Message &message,
                                                          uint64_t deliveryTag, bool redelivered) {
            auto body_ = message.body();
            std::cout << " [.] Received (" << body_ << ")" << std::endl;

            Range range1 = deserialize<Range>(body_);
            body(range1);
            client.channel_consume.ack(deliveryTag);
        });
    }


};

#endif //SHEEPPL_PARALLEL_FOR_H
