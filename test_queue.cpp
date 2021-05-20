//
// Created by ytymchenko on 08.05.2021.
//
#include <blocked_range.h>
#include <thread>
#include "concurrent_queue.h"
#include "blocked_container.h"
#include "pipeline.h"
#include <string>
#include "utils.h"
#include "configuration.h"



int main(int argc, char *argv[]) {


    if (argc < 2) {
        std::cerr << "Please specify the config path" << std::endl;
        return -1;
    };

    init(std::string(argv[1]));

//
//    boost::asio::io_service io_service_;
//    boost::asio::io_service::work work(io_service_);
//    std::thread thread([&io_service_]() { io_service_.run(); });;
//    RabbitClient client(io_service_);
//    client.channel_consume.setQos(1);
//
//
    auto i = 0;
    auto d = sheeppl::make_filter<void, std::string>(sheeppl::filter::serial_in_order,
                                                     [&](sheeppl::flow_control &fc) -> std::string {
                                                         for (; i < 5;) {
                                                             ++i;
                                                             return std::string("Hello");
                                                         }
                                                         fc.stop();
                                                         return std::string("d");
                                                     });
//
//    auto m = sheeppl::make_filter<std::string, std::string>(sheeppl::filter::parallel,
//                                                            [&](const std::string &filename) -> std::string {
//                                                                std::cout << filename << std::endl;
//                                                                return std::string("Never Code");
//
//                                                            });
//
//
//    auto q = sheeppl::make_filter<std::string, void>(sheeppl::filter::serial_in_order,
//                                                     [&](const std::string &filename) {
//                                                         std::cout << filename;
//
//                                                     });
//
//    auto dq = d & m & q;
//
//    sheeppl::parallel_pipeline( dq, client);
//
//
//    client.close();
//    thread.join();


    auto f = create_data([&]() {return std::string("helllllll");});
    std::cout<<f;

    return 0;
};