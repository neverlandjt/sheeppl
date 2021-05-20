//
// Created by ytymchenko on 04.05.2021.
//

#ifndef SHEEPPL_COMMUNICATION_H
#define SHEEPPL_COMMUNICATION_H

#include "RabbitClient.h"
#include <boost/format.hpp>
#include "configuration.h"
#include "amqpcpp.h"

namespace sheeppl {

    void send_to(RabbitClient &client, std::string to_, AMQP::Envelope &&env);

    void receive(RabbitClient &client);

}

#endif //SHEEPPL_COMMUNICATION_H
