//
// Created by ytymchenko on 30.04.2021.
//

#ifndef SHEEPPL_PUBLISH_H
#define SHEEPPL_PUBLISH_H

#include "RabbitClient.h"

namespace sheeppl {

    void publish(RabbitClient &client) {
        client.channel_publish.declareExchange("topic", AMQP::topic);
        client.channel_publish.publish("topic", "try.try", "123");
    }

}


#endif //SHEEPPL_PUBLISH_H
