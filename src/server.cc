#include "rpc/server.h"
#include "key_value.h"
#include <string>
using namespace std;

int main() {
  rpc::server srv(8080);

  KeyValueStore<int> kv;

  srv.bind("get", [&kv](string key){ return kv.get(key); });
  srv.bind("put", [&kv](string key, int val){ kv.put(key, val); });
  srv.bind("remove", [&kv](string key){ kv.remove(key); });

  srv.run();
  return 0;
}
