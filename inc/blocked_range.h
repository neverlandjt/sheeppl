//
// Created by ytymchenko on 21.03.2021.
//

#ifndef SHEEPPL_BLOCKED_RANGE_H
#define SHEEPPL_BLOCKED_RANGE_H

#include <iostream>
#include <fstream>

// include headers that implement a archive in simple text format
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/access.hpp>
#include "sheeppl_stddef.h"
#include "configuration.h"


namespace sheeppl {
    template<typename Value>
    class blocked_range {
    public:

        typedef size_t size_type;
        typedef Value const_iterator;

        blocked_range() { blocked_range(0, 0, 0); }

        blocked_range(const blocked_range &r) : my_end(r.end()), my_begin(r.begin()), my_grain_size(r.grain_size()) {}

        blocked_range(Value begin_, Value end_, size_type grain_size_ = -1) :
                my_end(end_), my_begin(begin_),
                my_grain_size(grain_size_ ? configuration::getInstance()->getData().grain_size : 100) {}

        blocked_range(blocked_range &r, split) :
                my_end(r.my_end),
                my_begin(do_split(r)),
                my_grain_size(r.my_grain_size) {};


        template<class Archive>
        void serialize(Archive &ar, const unsigned int version) {
            ar & my_begin;
            ar & my_end;
        }

        std::string str() const {
            std::ostringstream oss;
            oss << "{" << my_begin << "," << my_end << "}";
            return oss.str();
        }

        std::pair<blocked_range<Value>, blocked_range<Value>> split_() const {
            Value middle = (size() / 2u) + my_begin;
            return std::pair<blocked_range<Value>, blocked_range<Value>>
                    (blocked_range<Value>(my_begin, middle, my_grain_size),
                     blocked_range<Value>(middle, my_end, my_grain_size));
        }

        size_type size() const { return size_type(my_end - my_begin); }

        bool empty() const { return my_begin >= my_end; }

        size_type grain_size() const { return my_grain_size; }

        bool is_divisible() const { return my_grain_size < size(); }

        const_iterator begin() const { return my_begin; }

        const_iterator end() const { return my_end; }

    private:
        friend class boost::serialization::access;

        Value my_end;
        Value my_begin;
        size_type my_grain_size{};
    };
}


#endif //SHEEPPL_BLOCKED_RANGE_H
