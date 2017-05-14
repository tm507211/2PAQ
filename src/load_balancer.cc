#include "rpc/server.h"
#include <iostream>
#include <vector>
#include <string>
#include <utility>
using namespace std;

int main(int argc, char ** argv){
  if (argc != 2){
    cerr << "Usage : " << argv[0] << " <PORT_NUMBER>" << endl;
    return -1;
  }

  vector<pair<string, size_t>> servers;
  vector<size_t> used;
  double put_percent;
  double rem_percent;
  size_t data_size;

  rpc::server server(stoi(argv[1]));

  cin >> put_percent;
  if (put_percent < 0 || put_percent > 1){
    cerr << "Percentage of PUTs must be between 0 and 1" << endl;
    return -1;
  }
  cin >> rem_percent;
  if (rem_percent < 0 || put_percent + rem_percent > 1){
    cerr << "Percentage of removes must be between 0 and 1 and (PUTS + REMOVES) must be <= 1" << endl;
    return -1;
  }
  cin >> data_size;
  int n;
  cin >> n;
  if (n <= 0){
    cerr << "You must specify a positive number of servers" << endl;
    return -1;
  }
  for (size_t i = 0; i < n; ++i){
    string ip;
    size_t port;
    cin >> ip >> port;
    servers.push_back(make_pair(ip, port));
  }
  used.resize(n, 0);

  server.bind("get_put_percent", [&put_percent](){ return put_percent; });
  server.bind("get_rem_percent", [&rem_percent](){ return rem_percent; });
  server.bind("get_size", [&data_size](){ return data_size; });
  server.bind("choose_node", [&used, &servers](string ip, size_t port){
      if (servers.size() == 1){
	return servers[0];
      }
      int curr = -1;
      if (ip != ""){
	for (size_t i = 0; i < servers.size(); ++i){
	  if (servers[i] == make_pair(ip, port)){
	    curr = i;
	    break;
	  }
	}
	--used[curr];
      }
      size_t i = 0;
      if (curr == 0) ++i;
      size_t min = used[i];
      size_t min_i = i++;
      for (; i < used.size(); ++i){
	if (i != curr && used[i] < min){
	  min = used[i];
	  min_i = i;
	}
      }
      ++used[min_i];
      return servers[min_i];
    });

  cout << "STARTED LOAD BALANCER" << endl;
  server.run();
  
  return 0;
}
