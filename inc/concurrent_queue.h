//
// Created by ytymchenko on 08.05.2021.
//

#ifndef SHEEPPL_CONCURRENT_QUEUE_H
#define SHEEPPL_CONCURRENT_QUEUE_H

#include <deque>
#include <mutex>
#include <condition_variable>


template<typename T>
class concurrent_queue {
private:
    std::deque<T> q;
    std::mutex m;
    std::condition_variable cv;
public:

    concurrent_queue()= default;
    void push_back(T item) {
        {
            std::lock_guard<std::mutex> lck(m);
            q.push_back(std::move(item));
        }
        cv.notify_one();
    }

    T pop_front() {
        std::unique_lock<std::mutex> lck(m);
        cv.wait(lck, [this] { return !q.empty(); });
        T rv = std::move(q.front());
        q.pop_front();
        return std::move(rv);
    }

    std::pair<T, T> pop_front_pair() {
        std::unique_lock<std::mutex> lck(m);
        cv.wait(lck, [this] { return q.size() > 1; });
        T first = std::move(q.front());
        q.pop_front();
        T second = std::move(q.front());
        q.pop_front();
        return std::pair<T, T>{std::move(first), std::move(second)};
    }
};

#endif //SHEEPPL_CONCURRENT_QUEUE_H
