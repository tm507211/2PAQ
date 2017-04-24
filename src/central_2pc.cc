/*************************************************************************************
    Author: Charlie Murphy
    Email:  tcm3@cs.princeton.edu

    Date:   April 23, 2017

    Description: A simple centralized server to help start up 2 Phase Commit
 *************************************************************************************/

#include "rpc/server.h"
#include <string>
#include <iostream>
using namespace std;

int main(int argc, char ** argv){
  string leader="";
  size_t leader_port(8080);

  if (argc == 2){
    leader_port = stoi(argv[1]);
  }
  rpc::server server(leader_port);
  
  server.bind("leader", [&leader, &leader_port](string address, size_t port){
      if (leader == ""){
	leader = address;
	leader_port = port;
      }
      return make_pair(leader, leader_port);
    });
  server.run();
}
