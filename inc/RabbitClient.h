//
// Created by ytymchenko on 21.03.2021.
//

#ifndef SHEEPPL_RABBITCLIENT_H
#define SHEEPPL_RABBITCLIENT_H

#include <iostream>
#include <boost/asio/io_service.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/deadline_timer.hpp>

#include <amqpcpp.h>
#include <amqpcpp/libboostasio.h>
#include "configuration.h"

class RabbitClient {
public:
    AMQP::LibBoostAsioHandler *handler;
    AMQP::TcpConnection *connection;
    AMQP::TcpChannel channel_consume;
    AMQP::TcpChannel channel_publish;


    explicit RabbitClient(boost::asio::io_service &io_service_) :
            handler(new AMQP::LibBoostAsioHandler(io_service_)),
            connection(
                    new AMQP::TcpConnection(handler, configuration::getInstance()->getData().rabbit_connection_string)),
            channel_consume(AMQP::TcpChannel(connection)),
            channel_publish(AMQP::TcpChannel(connection)),
            receive_(configuration::getInstance()->getData().receiver),
            io_service_(io_service_) {}

    ~RabbitClient() {
        if (!connection->closed())
            connection->close();
//        delete channel_consume;
        delete connection;
        delete handler;
    }

    bool receive() const {
        return receive_;
    }

    void close() {
        if (!receive_) {

            if (!connection->closed()) {
                connection->close();
            }

            while (!connection->closed()) {}
            io_service_.stop();
        }

    }

private:
    bool receive_ = true;
    boost::asio::io_service &io_service_;
};


#endif //SHEEPPL_RABBITCLIENT_H
