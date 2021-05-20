//
// Created by ytymchenko on 21.03.2021.
//

#include <iostream>
#include <vector>
#include "parallel_for.h"
#include "RabbitClient.h"
#include "blocked_range.h"
#include "parallel_reduce.h"
#include<boost/program_options.hpp>
#include "communication.h"
#include "utils.h"
#include "configuration.h"
#include "blocked_container.h"

#include <vector>
#include <string>
#include <fstream>
#include <algorithm>
#include <iomanip>
#include <boost/locale.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <utils.h>

namespace bl = boost::locale;
using dict_ = std::unordered_map<std::string, size_t>;

#include <chrono>
#include <atomic>

inline std::chrono::high_resolution_clock::time_point get_current_time_fenced() {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto res_time = std::chrono::high_resolution_clock::now();
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return res_time;
}

template<class D>
inline long long to_us(const D &d) {
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}


class count_words {
public:
    dict_ dict;;

    void operator()(const sheeppl::blocked_container<std::vector<std::string>> &words) {
        for (auto &word: words) {
            dict[word]++;
        }
    }

    template<class Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & dict;
    }

    void join(count_words &rhs) {
        for (auto&[key, val]: rhs.dict) {
            dict[key] += val;
        }
    }

    friend class boost::serialization::access;


    count_words() : dict({}) {}
};

std::string read_txt(std::string infile) {
    std::ifstream file(infile);
    auto ss = std::ostringstream{};
    ss << file.rdbuf();
    return ss.str();
}

std::vector<std::string> split(std::string data) {
    std::vector<std::string> words;
    boost::locale::generator gen;
    std::locale::global(gen(""));
    bl::boundary::ssegment_index map(bl::boundary::word, data.begin(), data.end(), gen(""));
    map.rule(bl::boundary::word_letters);
    std::string word;
    for (bl::boundary::ssegment_index::iterator it = map.begin(), e = map.end(); it != e; ++it) {
        word = *it;
        word = bl::normalize(word, bl::norm_default);
        word = bl::fold_case(word);
        words.push_back(word);
    }
    return words;
}


auto countWords(std::vector<std::string> &f, RabbitClient &client) {
    count_words total;
    sheeppl::parallel_reduce(sheeppl::blocked_container<std::vector<std::string> >(f, f.size()),
                             total, client);
    return total.dict;
}

int main(int argc, char *argv[]) {


    if (argc < 2) {
        std::cerr << "Please specify the config path" << std::endl;
        return -1;
    };

    init(std::string(argv[1]));


    boost::asio::io_service io_service_;
    boost::asio::io_service::work work(io_service_);
    std::thread thread([&io_service_]() { io_service_.run(); });
    RabbitClient client(io_service_);
    client.channel_consume.setQos(1);


    auto array = create_data([&]() {
        std::string text = read_txt("../test.txt");
        std::vector<std::string> words;
        try {
            words = split(text);

        } catch (const std::runtime_error &e) {
            std::cerr << e.what() << std::endl;
        }
        return words;
    });

    auto start_time = get_current_time_fenced();
    auto res = countWords(array, client);
    auto end_time = get_current_time_fenced();
    auto final = to_us(end_time - start_time);
    std::cout<<final;

    client.close();
    thread.join();
    return 0;
//    return io_service_.run();
}

