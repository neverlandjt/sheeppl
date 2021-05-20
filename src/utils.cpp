//
// Created by ytymchenko on 04.05.2021.
//

#include <iostream>
#include <fstream>
#include "utils.h"


void init(std::string &&path) {

    std::ifstream configure(path);
    if (!configure.is_open()) {
        std::cerr << "Failed to open configuration file " << path << std::endl;
    }
    configuration *s = configuration::getInstance();
    try {
        s->setData(configure);
    } catch (std::exception &ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
    }


}