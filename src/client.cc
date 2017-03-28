#include "rpc/client.h"
#include <iostream>
#include <string>
using namespace std;

int main() {
  std::string ip_address;
  cin >> ip_address;
  rpc::client c(ip_address, 8080);

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
      c.call(action);
    } else {
      cerr << "invalid action: " << action << endl;
    }
  }
  return 0;
}
