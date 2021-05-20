////
//// Created by ytymchenko on 04.05.2021.
////
//#include "communication.h"
//
//
//void sheeppl::send_to(RabbitClient &client, std::string to_, AMQP::Envelope &&env) {
//    const std::string &node_id = configuration::getInstance()->getData().node_id;
//    client.channel_publish.declareExchange("topic", AMQP::topic);
//    client.channel_publish.declareQueue(to_);
//    client.channel_publish.bindQueue("topic", to_, (boost::format("%1%.*") % to_).str());
//    client.channel_publish.publish("topic", (boost::format("%1%.%2%") % to_ % node_id).str(), env);
//}
//
//void sheeppl::receive(RabbitClient &client) {
//    const std::string &node_id = configuration::getInstance()->getData().node_id;
//    client.channel_consume.declareQueue(node_id);
//    client.channel_consume.consume(node_id).onReceived([&](const AMQP::Message &message,
//                                                           uint64_t deliveryTag, bool redelivered) {
//        auto body_ = message.body();
//        std::cout << " [.] Received (" << body_ << ") (" << message.routingkey() << ")" << std::endl;
//
//        client.channel_consume.ack(deliveryTag);
//    });
//}
