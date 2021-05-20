# cmake_minimum_required(VERSION <specify CMake version here>)
cmake_minimum_required(VERSION 3.10)
project(sheeppl)

set(CMAKE_CXX_STANDARD 17)

#find_package(TBB REQUIRED)

find_package(Boost REQUIRED serialization program_options locale log)

include_directories(${Boost_INCLUDE_DIRS} inc/ src/)
link_directories(${Boost_LIBRARY_DIRS})
message([STATUS] ${Boost_LIBRARY_DIRS})

#Here one should include and link the AMQP-CPP
#include_directories( )
#link_directories()


file(GLOB headers inc/*.h)
file(GLOB sources src/"*.cpp")
find_package(ICU REQUIRED COMPONENTS data i18n io test tu uc)


add_executable(test_for test_reduce.cpp src/RabbitClient.cpp inc/RabbitClient.h inc/parallel_for.h inc/publish.h inc/communication.h src/utils.cpp inc/utils.h src/configuration.cpp inc/configuration.h src/communication.cpp inc/sheeppl_stddef.h src/blocked_container.cpp inc/blocked_container.h inc/concurrent_queue.h inc/parallel_reduce.h src/pipeline.cpp inc/pipeline.h)
add_executable(test_queue test_queue.cpp src/RabbitClient.cpp inc/RabbitClient.h inc/parallel_for.h inc/publish.h inc/communication.h src/utils.cpp inc/utils.h src/configuration.cpp inc/configuration.h src/communication.cpp inc/sheeppl_stddef.h src/blocked_container.cpp inc/blocked_container.h inc/concurrent_queue.h inc/parallel_reduce.h src/pipeline.cpp inc/pipeline.h)


target_link_libraries(test_for amqpcpp ${Boost_LIBRARIES} ${ICU_LIBRARIES} boost_system pthread dl ssl)
target_link_libraries(test_queue amqpcpp ${Boost_LIBRARIES} ${ICU_LIBRARIES} boost_system pthread dl ssl)

