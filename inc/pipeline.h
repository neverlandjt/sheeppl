//
// Created by ytymchenko on 10.05.2021.
//

#ifndef SHEEPPL_PIPELINE_H
#define SHEEPPL_PIPELINE_H

#include <variant>
#include "concurrent_queue.h"
#include "pipeline.h"
#include "RabbitClient.h"
#include <deque>
#include <boost/log/trivial.hpp>
#include <boost/format.hpp>
#include "utils.h"


namespace sheeppl {
    class pipeline;

    class filter;

    class queue_helper;

    template<typename T>
    class value_wrapper {
    public:
        T value;

        value_wrapper(T &&val) {
            value = val;
        };

        void get(T &value_) {
            value_ = std::move(value);
            delete this;
        }
    };


    template<typename T>
    class pointer_helper {

    public:

        static void *cast_to_void_ptr(T &&ref) {
            value_wrapper<T> *wrapped_ref = new value_wrapper<T>(std::move(ref));

            return static_cast<void *>(wrapped_ref);
        }

        static T cast_from_void_ptr(void *ref) {
            value_wrapper<T> *wrapped_ref = static_cast<value_wrapper<T> *>(ref);
            T val;
            wrapped_ref->get(val);
            return val;
        }


    };


    class filter {
        friend class pipeline;

        friend class queue_helper;

    protected:
        const unsigned char my_filter_mode;
        size_t ref_count = 0;
        size_t id = 0;
        size_t token = 0;
        std::shared_ptr<queue_helper> input_queue;
        std::shared_ptr<queue_helper> output_queue;

        filter *next_filter_in_pipeline;
        filter *prev_filter_in_pipeline;
        pipeline *my_pipeline;


        static filter *not_in_pipeline() { return reinterpret_cast<filter *>(intptr_t(-1)); }


        RabbitClient *client;
        static const unsigned char filter_is_out_of_order = 0x1 << 4;
        static const unsigned char filter_is_serial = 0x1;
    public:
        bool is_serial() const {
            return bool(my_filter_mode & filter_is_serial);
        };

        bool is_ordered() const {
            return (my_filter_mode & (filter_is_out_of_order | filter_is_serial)) == filter_is_serial;
        };

        virtual void operator()() = 0;

        enum mode {
            parallel = filter_is_out_of_order,
            serial_in_order = filter_is_serial,
            serial_out_of_order = filter_is_serial | filter_is_out_of_order,
        };

        explicit filter(mode filter_mode) :
                next_filter_in_pipeline(not_in_pipeline()),
                prev_filter_in_pipeline(not_in_pipeline()),
                my_pipeline(nullptr),
                client(nullptr),
                my_filter_mode(filter_mode) {};


    };


    class queue_helper {
        typedef std::variant<std::deque<void *, std::allocator<void *>>, concurrent_queue<void *>> queue_var;
    public:
        explicit queue_helper(const unsigned char &mode) {
            if ((mode & (filter::filter_is_out_of_order | filter::filter_is_serial)) == filter::filter_is_serial)
                queue.template emplace<std::deque<void *>>();
            else
                queue.template emplace<concurrent_queue<void *>>();
        };
        queue_var queue;

        void *get() {
            void *t = std::visit(get_item{}, queue);
            --ref_count;
            return t;
        };

        void put(void *item) {
            std::visit(put_item{item}, queue);
            ++ref_count;
        };

        bool processing_finished() {
            return (!is_consuming_ & !(ref_count > 0));
        }

        void stop_consuming() {
            is_consuming_ = false;
        }

    private:
        int ref_count = 0;
        bool is_consuming_ = true;


        struct get_item {
            void *operator()(std::deque<void *> &q) {
                void *rv = std::move(q.front());
                q.pop_front();
                return std::move(rv);
            }

            void *operator()(concurrent_queue<void *> &q) {
                return q.pop_front();
            };
        };

        struct put_item {
            void *item;

            void operator()(std::deque<void *> &q) {
                q.push_back(std::move(item));
            }

            void operator()(concurrent_queue<void *> &q) {
                return q.push_back(item);
            }
        };


    };


    class flow_control {
        bool is_pipeline_stopped;

        flow_control() { is_pipeline_stopped = false; }

        template<typename T, typename U, typename Body> friend
        class concrete_filter;

    public:
        void stop() { is_pipeline_stopped = true; }
    };


    template<typename I, typename O, typename Body>
    class concrete_filter : public filter {
        const Body &my_body;
        typedef queue_helper i_helper;
        typedef queue_helper o_helper;
        typedef pointer_helper<I> pi_helper;
        typedef pointer_helper<O> po_helper;


        void operator()() override {
            output_queue = std::shared_ptr<o_helper>(new o_helper(my_filter_mode));
            if (!is_serial()) {
                return this->operator()(true);
            }


            I input;
            while (!input_queue->processing_finished()) {
                input = pi_helper::cast_from_void_ptr(input_queue->get());
                output_queue->put(po_helper::cast_to_void_ptr(my_body(input)));
            }

            output_queue->stop_consuming();
        }

        void operator()(bool parallel) {
            if (configuration::getInstance()->getData().receiver) {
                client->channel_consume.declareQueue((boost::format("result_%1%") % id).str());
                client->channel_consume.declareQueue((boost::format("task_%1%") % id).str());
                client->channel_consume.consume((boost::format("task_%1%") % id).str()).onReceived(
                        [&client = (this->client), this](const AMQP::Message &message,
                                                         uint64_t deliveryTag,
                                                         bool redelivered) mutable {
                            auto body_ = message.body();
                            BOOST_LOG_TRIVIAL(info) << " [.] Received (" << message.correlationID() << " )";
                            const I input = deserialize<I>(body_);
                            O output = my_body(input);
                            auto s = serialize(output);
                            AMQP::Envelope env(s.c_str(), s.size());
                            env.setCorrelationID(message.correlationID());
                            client->channel_consume.publish("", message.replyTo(), env);
                            client->channel_consume.ack(deliveryTag);
                        });
//                return nullptr;
            } else {

                while (!input_queue->processing_finished()) {
                    auto data = pi_helper::cast_from_void_ptr(input_queue->get());
                    auto data_string = serialize(data);
                    AMQP::Envelope env(data_string.c_str(), data_string.size());
                    env.setCorrelationID(data_string);
                    env.setReplyTo((boost::format("result_%1%") % id).str());
                    client->channel_publish.publish("", (boost::format("task_%1%") % id).str(), env);
                }

                client->channel_consume.consume((boost::format("result_%1%") % id).str(), "2")
                        .onReceived(
                                [&client = (this->client), body = this->my_body, helper = output_queue, &token = this->token](
                                        const AMQP::Message &message,
                                        uint64_t deliveryTag,
                                        bool redelivered) mutable {

                                    try {
                                        auto *val = po_helper::cast_to_void_ptr(deserialize<O>(message.body()));
                                        helper->put(std::move(val));

                                    }
                                    catch (std::exception &exc) {
                                        std::cerr << "err:" << exc.what() << std::endl;
                                    }
                                    client->channel_consume.ack(deliveryTag);

                                    std::cout << --token << "\t";
                                    if (token == 1)
                                        helper->stop_consuming();


                                    if (token == 0) {
                                        std::cout << "here my token ifka";
                                        client->channel_consume.cancel("2");

                                    }

                                });
            }
        }


    public:
        concrete_filter(filter::mode
                        filter_mode,
                        const Body &body
        ) :

                filter(filter_mode), my_body(body) {}

    };


//     output
    template<typename I, typename Body>
    class concrete_filter<I, void, Body> : public filter {
        const Body &my_body;
        typedef queue_helper i_helper;
        typedef pointer_helper<I> pi_helper;


        void operator()() override {
//            auto *temp_input = (i_helper *) input;
            int od = 0;
            my_body(pi_helper::cast_from_void_ptr(input_queue->get()));
            while (!input_queue->processing_finished() && --token > 0) {
                my_body(pi_helper::cast_from_void_ptr(input_queue->get()));

            }

        }


    public:
        concrete_filter(filter::mode filter_mode, const Body &body) : filter(filter_mode), my_body(body) {}
    };


    // input
    template<typename U, typename Body>
    class concrete_filter<void, U, Body> : public filter {
        const Body &my_body;
        typedef queue_helper o_helper;
        typedef pointer_helper<U> po_helper;


        void operator()() override {
            flow_control control;
            auto temp = po_helper::cast_to_void_ptr(my_body(control));
            while (!control.is_pipeline_stopped) {
                ++token;
                output_queue->put(std::move(temp));
                temp = po_helper::cast_to_void_ptr(my_body(control));
            }

            output_queue->stop_consuming();
        }

    public:
        concrete_filter(filter::mode filter_mode, const Body &body) :
                filter(filter_mode), my_body(body) {}
    };


    class filter_node {
        std::atomic<intptr_t> ref_count;
    protected:
        filter_node() {
            ref_count = 0;
        }

    public:
        virtual void add_to(pipeline &) = 0;

        void add_ref() { ++ref_count; }

        void remove_ref() {
            --ref_count;
        }

    };


    class pipeline {
    public:
        void run() {
            size_t id = 0;
            filter_list->output_queue = std::shared_ptr<queue_helper>(new queue_helper(filter_list->my_filter_mode));

            filter_list->operator()();
            size_t tokens = filter_list->token;
            for (filter *f = filter_list->next_filter_in_pipeline; f; f = f->next_filter_in_pipeline) {
                f->input_queue = f->prev_filter_in_pipeline->output_queue;
                f->id = ++id;
                f->token = tokens;
                std::cout << "Task in chain " << f->id << std::endl;
                if (configuration::getInstance()->getData().receiver && !f->is_serial()) {
                    std::cout << "Not serial";
                    f->operator()();
                } else if (!configuration::getInstance()->getData().receiver) {
                    f->operator()();
                }

            }
        };

        pipeline() : filter_list(nullptr),
                     filter_end(nullptr),
                     client(nullptr) {};

        explicit pipeline(RabbitClient &client_) : filter_list(nullptr),
                                                   filter_end(nullptr),
                                                   client(&client_) {};

        void add_filter(filter &filter_) {
            filter_.my_pipeline = this;
            filter_.prev_filter_in_pipeline = filter_end;
            if (filter_list == nullptr)
                filter_list = &filter_;
            else
                filter_end->next_filter_in_pipeline = &filter_;
            filter_.next_filter_in_pipeline = nullptr;
            filter_end = &filter_;
            filter_.client = this->client;
        }

    private:
        friend class filter;

        friend class pipeline_proxy;

        RabbitClient *client;

        filter *filter_list;
        filter *filter_end;


    };


    template<typename T, typename U, typename Body>
    class filter_node_leaf : public filter_node {
        const filter::mode mode;
        const Body body;

        void add_to(pipeline &p) override {
            concrete_filter<T, U, Body> *f = new concrete_filter<T, U, Body>(mode, body);
            p.add_filter(*f);
        }

    public:
        filter_node_leaf(filter::mode m, const Body &b) : mode(m), body(b) {}
    };


    class filter_node_join : public filter_node {
        friend class filter_node;

        filter_node &left;
        filter_node &right;

        ~filter_node_join() {
            left.remove_ref();
            right.remove_ref();
        }

        void add_to(pipeline &p) override {
            left.add_to(p);
            right.add_to(p);
        }

    public:
        filter_node_join(filter_node &x, filter_node &y) : left(x), right(y) {
            left.add_ref();
            right.add_ref();
        }
    };


    template<typename T, typename U>
    class filter_wrapper {
        filter_node *root;

        filter_wrapper(filter_node *root_) : root(root_) {
            root->add_ref();
        }

        friend class pipeline_proxy;

        template<typename T_, typename U_, typename Body>
        friend filter_wrapper<T_, U_> make_filter(filter::mode, const Body &);

        template<typename T_, typename V_, typename U_>
        friend filter_wrapper<T_, U_> operator&(const filter_wrapper<T_, V_> &, const filter_wrapper<V_, U_> &);

    public:
        filter_wrapper() : root(nullptr) {}

        filter_wrapper(const filter_wrapper<T, U> &rhs) : root(rhs.root) {
            if (root) root->add_ref();
        }

        template<typename Body>
        filter_wrapper(filter::mode mode, const Body &body) :
                root(new filter_node_leaf<T, U, Body>(mode, body)) {
            root->add_ref();
        }

        void operator=(const filter_wrapper<T, U> &rhs) {

            filter_node *old = root;
            root = rhs.root;
            if (root) root->add_ref();
            if (old) old->remove_ref();
        }

        ~filter_wrapper() {
            if (root) root->remove_ref();
        }

        void clear() {
            if (root) {
                filter_node *old = root;
                root = nullptr;
                old->remove_ref();
            }
        }
    };


    template<typename T, typename U, typename Body>
    filter_wrapper<T, U> make_filter(filter::mode mode, const Body &body) {
        return new filter_node_leaf<T, U, Body>(mode, body);
    }

    template<typename T, typename V, typename U>
    filter_wrapper<T, U> operator&(const filter_wrapper<T, V> &left, const filter_wrapper<V, U> &right) {
        return new filter_node_join(*left.root, *right.root);
    }


    class pipeline_proxy {
    public:

        pipeline_proxy(const filter_wrapper<void, void> &filter_chain, RabbitClient &client) : my_pipe(client) {
            filter_chain.root->add_to(my_pipe);
        }


        pipeline *operator->() { return &my_pipe; }

        pipeline my_pipe;

    };


    inline void parallel_pipeline(const filter_wrapper<void, void> &filter_chain, RabbitClient &client_) {
        pipeline_proxy pipe(filter_chain, client_);
        pipe->run();
    }

//    void parallel_pipeline_receiver(const filter_wrapper<void, void> &filter_chain, RabbitClient &client_) {}


}

#endif //SHEEPPL_PIPELINE_H
