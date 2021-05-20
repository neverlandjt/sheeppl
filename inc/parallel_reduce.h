//
// Created by ytymchenko on 09.05.2021.
//

#ifndef SHEEPPL_PARALLEL_REDUCE_H
#define SHEEPPL_PARALLEL_REDUCE_H

#include <iostream>
#include <mutex>
#include <thread>
#include "utils.h"
#include "concurrent_queue.h"
#include "blocked_range.h"
#include "RabbitClient.h"
#include <boost/log/trivial.hpp>

template<typename _Tp>
using remove_cvref_t
= typename std::remove_cv<typename std::remove_reference<_Tp>::type>::type;


namespace sheeppl {
    template<typename Body, typename Container>
    class parallel_reduce_impl {
    public:
        static void run(const Container &container, Body &body, RabbitClient &client) {
            parallel_reduce_impl<Body, Container>(container, body, client);
        }

    private:
        boost::uuids::random_generator generator{};
        Body &body;
        Container container;
        RabbitClient &client;
        size_t token = 0;

        parallel_reduce_impl(const Container &container, Body &body, RabbitClient &client) :
                container(container), body(body), client(client) {

            exec_reduce();
        }

        void exec_reduce();;

        void split_exec_reply(const blocked_range <size_t> &range, size_t &chunks_left, std::mutex &m,
                              concurrent_queue<Body> &queue);;


        void exec_task_reply(const Container &new_container, std::string &correlation_id);

        void receive_return();;

        void receive_result(concurrent_queue<Body> &queue);;


    };

    template<typename Body, typename Container>
    void
    parallel_reduce_impl<Body, Container>::receive_result(concurrent_queue<Body> &queue) {
        client.channel_consume.consume("", "tag")
                .onReceived([&client = (this->client), body = this->body, &queue, token = (this->token)](
                        const AMQP::Message &message,
                        uint64_t deliveryTag,
                        bool redelivered) mutable {
//                    BOOST_LOG_TRIVIAL(info) << "  " << message.correlationID();

                    try {
                        auto val = deserialize<remove_cvref_t<decltype(body)>>(message.body());
                        queue.push_back(std::move(val));
                    }
                    catch (std::exception &exc) {
                        std::cerr << "err:" << exc.what() << std::endl;
                    }
                    client.channel_consume.ack(deliveryTag);

                    if (!--token)
                        client.channel_consume.cancel("tag");
                });

    }

    template<typename Body, typename Container>
    void parallel_reduce_impl<Body, Container>::receive_return() {
        client.channel_consume.declareQueue("rpc_queue");
        client.channel_consume.consume("rpc_queue").onReceived([&client = (this->client)](const AMQP::Message &message,
                                                                                          uint64_t deliveryTag,
                                                                                          bool redelivered) mutable {

            auto body_ = message.body();
//            BOOST_LOG_TRIVIAL(info) << " [.] Received (" << message.correlationID() << " )";
            const Container range1 = deserialize<Container>(body_);
            Body b;
            b.operator()(range1);
            auto s = serialize(b);
            AMQP::Envelope env(s.c_str(), s.size());
            env.setCorrelationID(message.correlationID());
            client.channel_publish.publish("", message.replyTo(), env);
            client.channel_consume.ack(deliveryTag);
        });
    }

    template<typename Body, typename Container>
    void
    parallel_reduce_impl<Body, Container>::exec_task_reply(const Container &new_container,
                                                           std::string &correlation_id) {
        ++token;
        auto data_string = serialize(new_container);
        AMQP::Envelope env(data_string.c_str(), data_string.size());
        env.setCorrelationID(correlation_id);
        env.setReplyTo("rpc_result");
        client.channel_publish.publish("", "rpc_queue", env);
    }

    template<typename Body, typename Container>
    void parallel_reduce_impl<Body, Container>::exec_reduce() {
        client.channel_consume.declareQueue("rpc_queue");
        client.channel_consume.declareQueue("rpc_result");
        if (client.receive())
            receive_return();
        else {
            blocked_range<size_t> range(0, container.container.size());
            uint8_t threads_number = 2;

            std::vector<std::thread> counters;
            counters.reserve(threads_number);
            size_t chunks_left = 0;
            std::mutex m;
            concurrent_queue<Body> queue;
            split_exec_reply(range, chunks_left, m, queue);
            receive_result(queue);


            for (uint8_t i = 0; i < threads_number; ++i) {
                counters.emplace_back(std::thread(reduce_<Body>, std::ref(queue), std::ref(m),
                                                  std::ref(chunks_left)));
            }
            for (auto &th: counters) {
                th.join();
            }
            auto counter = queue.pop_front();
            body.join(counter);

        }
    }

    template<typename Body, typename Container>
    void
    parallel_reduce_impl<Body, Container>::split_exec_reply(const blocked_range <size_t> &range, size_t &chunks_left,
                                                            std::mutex &m, concurrent_queue<Body> &queue) {
        if (!range.is_divisible()) {

            {
                std::lock_guard<std::mutex> lck(m);
                //   add the job
                ++chunks_left;
            }

//            BOOST_LOG_TRIVIAL(info) << range.begin() << " " << range.end();
            auto it(container.container.begin());
            Container new_container(it + range.begin(), it + range.end());
            std::string correlation_id = boost::uuids::to_string(generator());
            exec_task_reply(new_container, correlation_id);
            return;
        }
        auto range_pair = range.split_();
        split_exec_reply(range_pair.first, chunks_left, m, queue);
        split_exec_reply(range_pair.second, chunks_left, m, queue);

    }

    template<typename Body, typename Container>
    void parallel_reduce(const Container &container, Body &body, RabbitClient &client) {
        parallel_reduce_impl<Body, Container>::run(container, body, client);

    }

}


#endif //SHEEPPL_PARALLEL_REDUCE_H
