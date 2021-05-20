//
// Created by ytymchenko on 21.03.2021.
//

#include <iostream>
#include <vector>
#include "parallel_for.h"
#include "RabbitClient.h"
#include "blocked_range.h"
#include<boost/program_options.hpp>
#include "communication.h"
#include "utils.h"
#include "configuration.h"

namespace po = boost::program_options;

struct mytask {
    explicit mytask(size_t n)
            : _n(n) {}

    size_t operator()() const {
        for (int i = 0; i < 2; ++i) {}  // Deliberately run slow
        std::cerr << "[" << _n << "]";
        return _n;
    }

    size_t _n = 0;
};

int main(int argc, char *argv[]) {
    char type;
    po::options_description all(
            "Usage: test_for [-h|--help] [--type=S|R]\n"
    );
    all.add_options()
            ("help,h", "display this help and exit")
            ("type", po::value<char>(&type), "type of execution:\n"
                                             "\t(R) receive (S) send");

    po::variables_map vm;
    try {
        po::store(po::command_line_parser(argc, argv).options(all).run(), vm);
        po::notify(vm);
    } catch (po::error &e) {
        std::cerr << "err: " << e.what() << std::endl << std::endl;
        exit(1);
    }

    if (vm.count("help")) {
        std::cout << all << std::endl;
        exit(0);
    }


    boost::asio::io_service io_service_;
    boost::asio::io_service::work work(io_service_);
    std::thread thread([&io_service_]() { io_service_.run(); });;
    init();
    RabbitClient client(io_service_, configuration::getInstance()->getData().rabbit_connection_string, type);
    client.channel_consume.setQos(1);
//    AMQP::Envelope env("e", 1);
//    sheeppl::send_to(client, "1", std::move(env));
//    sheeppl::receive(client);
    std::vector<mytask> tasks;

    for (int i = 0; i < 10; ++i)
        tasks.push_back(mytask(i));


    sheeppl::parallel_reduce(sheeppl::blocked_range<size_t>(1, 10),
                             [&tasks](const sheeppl::blocked_range<size_t> &r) {
                                 for (size_t i = r.begin(); i < r.end(); ++i) return tasks[i]();
                             }, client);




//    io_service_.stop();
    thread.join();
    return 0;
//    return io_service_.run();
}

