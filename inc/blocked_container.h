//
// Created by ytymchenko on 07.05.2021.
//

#ifndef SHEEPPL_BLOCKED_CONTAINER_H
#define SHEEPPL_BLOCKED_CONTAINER_H


#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/access.hpp>
#include "sheeppl_stddef.h"


namespace sheeppl {
    template<typename T>
    class blocked_container {
    public:
        // types
        typedef size_t size_type;
        typedef T const_iterator;

        blocked_container() = default;

        blocked_container(T &container, size_t size_) : container(container), size_(size_) {}


        blocked_container(typename T::const_iterator begin, typename T::const_iterator end) :
                container(T(begin, end)), size_(container.size()) {};

        template<class Archive>
        void serialize(Archive &ar, const unsigned int version) {
            ar & container;
            ar & size_;
        }

        auto begin() const {
            return container.begin();
        };

        auto end() const{
            return container.end();
        };

        T container;

    private:
        friend class boost::serialization::access;

        size_t size_;

    };
}

#endif //SHEEPPL_BLOCKED_CONTAINER_H
