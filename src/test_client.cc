#include "rpc/client.h"
#include <iostream>
#include <string>
#include <random>
#include <chrono>
#include <vector>
#include <utility>
using namespace std;

#define SECOND 1000000000
#define PRINT_TIME 1 * SECOND

string random_data(size_t size, mt19937& gen){
  static uniform_int_distribution<> distr(0, 25);
  string tmp;
  tmp.resize(size);
  for (size_t i = 0; i < size; ++i){
    tmp[i] = 'a' + distr(gen);
  }
  return tmp;
}

string get_key(vector<string>& keys, mt19937& gen, bool old = false){
  static uniform_real_distribution<> distr(0,1);
  double prob(0.05);
  if (keys.size() <= 10){
    prob = 1;
  } else if (keys.size() == 100 || old){
    prob = 0;
  }
  if (distr(gen) < prob){ /* generate new key */
    keys.push_back(random_data(100, gen));
    return keys[keys.size()-1];
  }
  uniform_int_distribution<> distr_int(0, keys.size()-1);
  return keys[distr_int(gen)];
}

int main(int argc, char ** argv){
  if (argc != 3){
    cerr << "Usage: " << argv[0] << " <load_balancer_ip> <load_balancer_port>";
    return -1;
  }
  rpc::client load_balancer(argv[1], stoi(argv[2]));

  pair<string, size_t> serv = load_balancer.call("choose_node", "", 0).template as<pair<string, size_t>>();
  cout << serv.first << " " << serv.second << endl;

  rpc::client *server = new rpc::client(serv.first, serv.second);
  while (server->get_connection_state() != rpc::client::connection_state::connected);
  
  double put_percent = load_balancer.call("get_put_percent").template as<double>();
  double rem_percent = load_balancer.call("get_rem_percent").template as<double>();
  size_t data_size = load_balancer.call("get_size").template as<size_t>();

  random_device rd;
  mt19937 gen(rd());
  uniform_real_distribution<> distr(0, 1);
  double prob;

  vector<string> keys;

  size_t time_get(0), num_get(0), get_min(-1), get_max(0);
  size_t time_since(0), time_last(0), time, time_units(1);
  while (1){
    auto start_loop = std::chrono::steady_clock::now();
    while (server->get_connection_state() != rpc::client::connection_state::connected){
      delete server;
      serv = load_balancer.call("choose_node", serv.first, serv.second).template as<pair<string, size_t>>();
      cout << serv.first << " " << serv.second << endl;
      server = new rpc::client(serv.first, serv.second);
      size_t loops = 0;
      while (loops++ <= 10 && server->get_connection_state() != rpc::client::connection_state::connected){
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
    prob = distr(gen);
    if (prob < put_percent){ /* Perform a put operation */
      server->call("put", get_key(keys, gen), random_data(data_size, gen));
    } else if(prob - put_percent < rem_percent) { /* Perform a remove operation */
      if (keys.size() == 0) continue;
      server->call("remove", get_key(keys, gen, true));
    } else { /* Perform a get operation */
      ++num_get;
      auto start = std::chrono::steady_clock::now();
      server->call("get", get_key(keys, gen)).as<string>();
      auto end = std::chrono::steady_clock::now();
      time = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
      time_get += time;
      get_min = (time < get_min) ? time : get_min;
      get_max = (time > get_max) ? time : get_max;
    }
    auto end_loop = std::chrono::steady_clock::now();
    time_since += std::chrono::duration_cast<std::chrono::nanoseconds>(end_loop - start_loop).count();
    if (time_since >= time_units * PRINT_TIME){
      cout << "TIME ELAPSED : " << 1.0 * (time_since - time_last) / SECOND << endl;
      cout << (get_min == -1 ? 0 : (1.0 * get_min / SECOND * 1000)) << " "
	   << (num_get ==  0 ? 0 : (1.0 * time_get / num_get / SECOND * 1000))
	   << " " << 1.0 * get_max / SECOND * 1000 << " "
	   << 1.0 * SECOND * num_get / (time_get ? time_get : 1) << endl;
      time_get = num_get = get_max = 0;
      get_min = -1;
      while (time_since >= time_units * PRINT_TIME){
	++time_units;
      }
      time_last = time_since;
    }
  }
  delete server;
  return 0;
}
