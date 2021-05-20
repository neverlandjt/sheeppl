//
// Created by ytymchenko on 01.04.2021.
//

#ifndef SHEEPPL_UTILS_H
#define SHEEPPL_UTILS_H

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/array.hpp>
#include <mutex>
#include "concurrent_queue.h"
#include "configuration.h"


#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <boost/asio/io_service.hpp>
//#include <thread>


template<typename Value>
std::string serialize(Value &obj) {
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << obj;
    return ss.str();
}

template<typename Value>
Value deserialize(const char *arr) {
    std::stringstream ss(arr);
    boost::archive::text_iarchive ia(ss);
    Value value;
    ia >> value;
    return value;
}


void init(std::string &&path);


template<typename Func, typename ... Types>
auto create_data(Func function_, Types &&... args) -> decltype(function_(std::forward<Types>(args)...)) {
    if (configuration::getInstance()->getData().receiver) {
        return  decltype(function_(std::forward<Types>(args)...)){};// Return type has to be default constructable
    } else
        return function_(std::forward<Types>(args)...);
}


template<typename T>
void reduce_(concurrent_queue<T> &queue, std::mutex &m, size_t &chunks_left) {
//    start reducing
    while (true) {
        {
            std::lock_guard<std::mutex> lck(m);
            if (chunks_left < 2) {
                break;
            }
            --chunks_left;
        }

        std::pair<T, T> dicts = std::move(queue.pop_front_pair());
        T first_dict = std::move(dicts.first);
        T second_dict = std::move(dicts.second);

        second_dict.join(first_dict);
        queue.push_back(std::move(second_dict));
    }
}

//template<typename Func, typename ... Types>
//void run_in_parallel(uint8_t threads_number, Func function_, Types &&... args) {
//    std::vector<std::thread> receivers;
//    receivers.reserve(threads_number);
//
//    for (uint8_t i = 0; i < threads_number; ++i) {
//        receivers.emplace_back(std::thread(function_, std::forward<Types>(args)...));
//    }
//    for (auto &th: receivers) {
//        th.join();
//    }
//}


#endif //SHEEPPL_UTILS_H
