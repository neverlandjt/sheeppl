//
// Created by ytymchenko on 04.05.2021.
//

#ifndef SHEEPPL_CONFIGURATION_H
#define SHEEPPL_CONFIGURATION_H


#include <fstream>
#include <map>
#include <string>
#include <iostream>
#include <fstream>
#include <algorithm>

class configuration {
    static configuration *instance;
    struct configuration_t {
        std::string rabbit_connection_string, node_id;
        std::uint32_t workers_per_node, grain_size;
        bool receiver;
    };
    configuration_t config;


    // Private constructor so that no objects can be created.
    configuration() {
        config = {};
    }

public:
    static configuration *getInstance() {
        if (!instance)
            instance = new configuration;
        return instance;
    }

    configuration_t getData() {
        return this->config;
    }

    void setData(std::istream &cf);
};


#endif //SHEEPPL_CONFIGURATION_H
