#include "rpc/client.h"
#include <iostream>
#include <string>
using namespace std;

int main() {
  std::string ip_address;
  size_t port;
  cin >> ip_address >> port;
  rpc::client c(ip_address, port);

  std::string key;
  std::string action;
  int val;
  while(cin >> action >> key){
    if (action == "put"){
      cin >> val;
      c.call(action, key, val);
    } else if (action == "get"){
      cout << "> " << key << " : " << c.call(action, key).as<int>() << endl;
    } else if (action == "remove"){
      c.call(action, key);
    } else {
      cerr << "invalid action: " << action << endl;
    }
  }
  return 0;
}
