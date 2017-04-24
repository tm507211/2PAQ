cmake_minimum_required(VERSION 3.0.0)
project(traq)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake-modules)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bin)
find_package(rpclib REQUIRED)

include_directories(${RPCLIB_INCLUDE_DIR})

add_executable(server src/server.cc)
target_link_libraries(server ${RPCLIB_LIBS} pthread)
set_target_properties(
        server
        PROPERTIES
        CXX_STANDARD 14
        COMPILE_FLAGS "${CMAKE_CXX_FLAGS} ${RPCLIB_EXTRA_FLAGS}")
target_compile_definitions(server PUBLIC ${RPCLIB_COMPILE_DEFINITIONS})

add_executable(client src/client.cc)
target_link_libraries(client ${RPCLIB_LIBS} pthread)
set_target_properties(
        client
        PROPERTIES
        CXX_STANDARD 14
        COMPILE_FLAGS "${CMAKE_CXX_FLAGS} ${RPCLIB_EXTRA_FLAGS}")
target_compile_definitions(client PUBLIC ${RPCLIB_COMPILE_DEFINITIONS})

add_executable(main2pc src/main_2pc.cc)
target_link_libraries(main2pc ${RPCLIB_LIBS} pthread)
set_target_properties(
        main2pc
        PROPERTIES
        CXX_STANDARD 14
        COMPILE_FLAGS "${CMAKE_CXX_FLAGS} ${RPCLIB_EXTRA_FLAGS}")
target_compile_definitions(main2pc PUBLIC ${RPCLIB_COMPILE_DEFINITIONS})

add_executable(central2pc src/central_2pc.cc)
target_link_libraries(central2pc ${RPCLIB_LIBS} pthread)
set_target_properties(
        central2pc
        PROPERTIES
        CXX_STANDARD 14
        COMPILE_FLAGS "${CMAKE_CXX_FLAGS} ${RPCLIB_EXTRA_FLAGS}")
target_compile_definitions(central2pc PUBLIC ${RPCLIB_COMPILE_DEFINITIONS})
