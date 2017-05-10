/*************************************************************************************
    Author: Charlie Murphy
    Email:  tcm3@cs.princeton.edu

    Date:   April 23, 2017

    Description: A simple implementation for Two Phase Commit with Apportioned Queries
 *************************************************************************************/
//#include "server_2pc.h"
//#include "serversion.h" //testAQ
#include "server_2pc_AQ.h"
#include <iostream>
#include <string>
#include <thread>
using namespace std;

int main(int argc, char ** argv){
  if (argc != 5){
    cerr << "Usage: " << argv[0] << " <address_of_local_machine> <port_number> <organizing_server_address> <port_number> " << endl;
    return -1;
  }
  Server<int> server(stoi(argv[2]));
  server.run(argv[1], stoi(argv[2]), argv[3], stoi(argv[4]));
  condition_variable cv;
  mutex m;
  unique_lock<std::mutex> lock(m);
  cv.wait(lock, []{return false;});
  return 0;
}
