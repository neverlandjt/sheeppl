//
// Created by ytymchenko on 04.05.2021.
//

#include "configuration.h"

configuration *configuration::instance = nullptr;

void configuration::setData(std::istream &cf) {
    std::string temp;
    while (getline(cf, temp)) {
        temp.erase(std::remove_if(temp.begin(), temp.end(), isspace),
                   temp.end());
        if (temp[0] == '#' || temp.empty())
            continue;
        auto delimiterPos = temp.find('=');
        std::string name = temp.substr(0, delimiterPos);
        auto value = temp.substr(delimiterPos + 1);
        std::transform(name.begin(), name.end(), name.begin(), ::tolower);
        value.erase(std::remove(value.begin(), value.end(), '"'), value.end());
        if (name == "rabbit_connection_string") {
            this->config.rabbit_connection_string = value;
        } else if (name == "node_id") {
            this->config.node_id = value;
        } else if (name == "workers_per_node") {
            this->config.workers_per_node = static_cast<uint32_t>(stoi(value));
        } else if (name == "node_type") {
            this->config.receiver = (value == "r");
        } else if (name == "grain_size") {
            this->config.grain_size =  static_cast<uint32_t>(stoi(value));
        }

    }
}
